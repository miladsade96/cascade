package cascade.group

private[cascade] final case class GroupAdminMember(
    memberId: String,
    groupInstanceId: Option[String],
    clientId: String,
    clientHost: String,
    metadata: Vector[Byte],
    assignment: Vector[Byte]
)

private[cascade] final case class GroupAdminDescription(
    groupId: String,
    state: String,
    protocolType: String,
    protocolData: String,
    members: Vector[GroupAdminMember]
)

/** Immutable administrative view derived only from acknowledged group state. */
private[cascade] final case class GroupAdminReadView private (
    imageVersion: Long,
    descriptions: Map[String, GroupAdminDescription]
):
  lazy val groups: Vector[GroupAdminDescription] = descriptions.valuesIterator.toVector.sortBy(_.groupId)

  def get(groupId: String): Option[GroupAdminDescription] = descriptions.get(groupId)

  def contains(groupId: String): Boolean = descriptions.contains(groupId)

private[cascade] object GroupAdminReadView:
  val Empty: GroupAdminReadView = from(GroupImage.Empty)

  def from(image: GroupImage): GroupAdminReadView =
    val classic = image.groups.iterator.map { group =>
      val description = GroupAdminDescription(
        group.groupId,
        stateName(group.status),
        group.protocolType,
        group.protocolName,
        group.members.map { member =>
          val metadata = member.protocols.find(_.name == group.protocolName).map(_.metadata).getOrElse(Vector.empty)
          GroupAdminMember(
            member.memberId,
            member.groupInstanceId,
            member.clientId,
            "",
            metadata,
            member.assignment
          )
        }
      )
      group.groupId -> description
    }.toMap
    val consumers = image.consumerGroups.iterator.map { group =>
      val description = GroupAdminDescription(
        group.groupId,
        if group.members.isEmpty then "Empty" else "Stable",
        "consumer",
        group.members.headOption.map(_.serverAssignor).getOrElse(""),
        group.members.map(member =>
          GroupAdminMember(member.memberId, member.instanceId, "", "", Vector.empty, Vector.empty)
        )
      )
      group.groupId -> description
    }.toMap
    val offsetOnly = image.offsets.iterator.map(_.key.groupId).toSet -- classic.keySet -- consumers.keySet
    val offsets = offsetOnly.iterator.map(groupId =>
      groupId -> GroupAdminDescription(groupId, "Empty", "", "", Vector.empty)
    ).toMap
    GroupAdminReadView(image.version, classic ++ consumers ++ offsets)

  private def stateName(status: GroupStatus): String = status match
    case GroupStatus.Empty               => "Empty"
    case GroupStatus.PreparingRebalance  => "PreparingRebalance"
    case GroupStatus.CompletingRebalance => "CompletingRebalance"
    case GroupStatus.Stable              => "Stable"
