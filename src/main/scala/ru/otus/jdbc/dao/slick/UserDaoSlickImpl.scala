package ru.otus.jdbc.dao.slick

import ru.otus.jdbc.model.{Role, User}
import slick.jdbc.PostgresProfile.api._

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}


class UserDaoSlickImpl(db: Database)(implicit ec: ExecutionContext) {

  import UserDaoSlickImpl._

  def getUser(userId: UUID): Future[Option[User]] = db.run(getUserAction(userId))

  private def getUserAction(userId: UUID) = {
    for {
      user <- users.filter(user => user.id === userId).result.headOption
      roles <- usersToRoles
        .filter(_.usersId === userId)
        .map(_.rolesCode)
        .result.map(_.toSet)
    } yield user.map(_.toUser(roles))
  }

  def createUser(user: User): Future[User] = db.run(createUseAction(user).transactionally)

  private def createUseAction(user: User) = for {
    userId <- (users returning users.map(_.id)) += UserRow.fromUser(user)
    _ <- usersToRoles ++= user.roles.map(userId -> _)
    createdUser = UserRow.fromUser(user).copy(id = Some(userId)).toUser(user.roles)
  } yield createdUser

  def updateUser(user: User): Future[Unit] = {
    user.id match {
      case Some(userId) =>
        val updateUser = users
          .filter(_.id === userId)
          .map(u => (u.firstName, u.lastName, u.age))
          .update((user.firstName, user.lastName, user.age))

        val deleteRoles = usersToRoles.filter(_.usersId === userId).delete
        val insertRoles = usersToRoles ++= user.roles.map(userId -> _)

        val action = updateUser >> deleteRoles >> insertRoles >> DBIO.successful(())

        db.run(action)
      case None => Future.successful(())
    }
  }

  def deleteUser(userId: UUID): Future[Option[User]] =
    getUser(userId)
      .flatMap {
        case Some(user) =>
          val deleteRoles = usersToRoles.filter(_.usersId === user.id).delete
          val deleteUser = users.filter(_.id === user.id).delete
          db.run(deleteRoles >> deleteUser).map(_ => Some(user))
        case _ => Future(None)
      }

  private def findByCondition(condition: Users => Rep[Boolean]): Future[Vector[User]] =
    db.run(findByConditionAction(condition))

  private def findByConditionAction(condition: Users => Rep[Boolean]) = {
    for {
      userSeq <- users.filter(condition).result
      roles <- usersToRoles.filter(_.usersId in users.filter(condition).map(_.id)).result
    } yield userSeq.map { userRow =>
      val userRoles = roles.filter(_._1 == userRow.id.get).map(_._2).toSet
      userRow.toUser(userRoles)
    }.toVector
  }

  def findByLastName(lastName: String): Future[Seq[User]] =
    findByCondition(_.lastName === lastName)

  def findAll(): Future[Seq[User]] = findByCondition(_ => true)

  private[slick] def deleteAll(): Future[Unit] = db.run(deleteAllAction().transactionally)

  private def deleteAllAction() = for {
    _ <- usersToRoles.delete
    _ <- users.delete
  } yield ()
}

object UserDaoSlickImpl {
  implicit val rolesType: BaseColumnType[Role] = MappedColumnType.base[Role, String](
    {
      case Role.Reader => "reader"
      case Role.Manager => "manager"
      case Role.Admin => "admin"
    },
    {
      case "reader" => Role.Reader
      case "manager" => Role.Manager
      case "admin" => Role.Admin
    }
  )


  case class UserRow(
                      id: Option[UUID],
                      firstName: String,
                      lastName: String,
                      age: Int
                    ) {
    def toUser(roles: Set[Role]): User = User(id, firstName, lastName, age, roles)
  }

  object UserRow extends ((Option[UUID], String, String, Int) => UserRow) {
    def fromUser(user: User): UserRow = UserRow(user.id, user.firstName, user.lastName, user.age)
  }

  class Users(tag: Tag) extends Table[UserRow](tag, "users") {
    val id = column[UUID]("id", O.PrimaryKey, O.AutoInc)
    val firstName = column[String]("first_name")
    val lastName = column[String]("last_name")
    val age = column[Int]("age")

    val * = (id.?, firstName, lastName, age).mapTo[UserRow]
  }

  val users: TableQuery[Users] = TableQuery[Users]

  class UsersToRoles(tag: Tag) extends Table[(UUID, Role)](tag, "users_to_roles") {
    val usersId = column[UUID]("users_id")
    val rolesCode = column[Role]("roles_code")

    val * = (usersId, rolesCode)
  }

  val usersToRoles = TableQuery[UsersToRoles]
}
