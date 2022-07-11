import java.sql.{Connection, DriverManager, SQLException}
import scala.collection.mutable.ListBuffer
object sql {
  //This class is for interacting with the MYSQL database. It is used to create a connection to the database, and to execute SQL queries.
  private var connection: Connection = _
  def connect(): Unit = {
    val url = "jdbc:mysql://localhost:3306/Users"
    val driver = "com.mysql.cj.jdbc.Driver"
    val username = "root"
    val password = "corey"

    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      //println("MySQL CONNECTION IS GOOD")

    } catch {
      case e: Exception => e.printStackTrace()
        println("something is wrong")
    }

  }




  def createUser(Username: String, Password: String): Unit = {
    connect()
    var resultSet = 0
    var statement = connection.prepareStatement(s"Insert into standard_users (Username, Password) Values(?, ?)")
    try {
      statement.setString(1, Username)
      statement.setString(2, Password)
      resultSet = statement.executeUpdate()
      println("The user account has been created")

    }
    catch {
      case e: SQLException => e.printStackTrace()
    }
  }

  def showAllUsers(): Unit = {
    connect()
    val statement = connection.prepareStatement("Select Username from standard_users")
    try {
      val resultSet = statement.executeQuery()

      while (resultSet.next()) {
        print(resultSet.getString("Username"))
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }


  def checkifExists(userCheck: String): Boolean = {
    connect()
    var bufferL = new ListBuffer[String]()

    val statement = connection.prepareStatement("SELECT Username FROM standard_users")
    val resultSet = statement.executeQuery()
    while (resultSet.next()) {
      bufferL += resultSet.getString("Username")
    }
    val userList = bufferL.toList
    val check = userList.contains(userCheck)
    if (check) {//user is detected
      true
    } else {//User is not detected
      false
    }
  }

  def updateStdPassword(username:String, password: String): Unit = {
    connect()
    val statement = connection.createStatement()

    try {
      statement.executeUpdate(s"UPDATE standard_users SET Password = '$password' WHERE Username = '$username'")
    } catch {
      case e: Exception => e.printStackTrace()
    }
    println("Password has been updated")

  }
  def updateAdmPassword(password: String): Unit = {
    connect()
    val statement = connection.createStatement()

    try {
      statement.executeUpdate(s"UPDATE admin_users SET Password = '$password' WHERE Username = 'Admin'")
    } catch {
      case e: Exception => e.printStackTrace()
    }
    println("Password has been updated")

  }


  def deleteUser(delUser: String): Unit = {
    connect()
    //showAllUsers()
    val statement = connection.createStatement().executeUpdate(s"Delete From standard_users where username = '$delUser'")

    if (statement == 0) {
      println("User does not exist, please try again")
    } else {
      println("User deleted")
    }
  }


  def validateLogin(usersUserName: String, usersPassword: String): Boolean = {
    connect()
    val statement = connection.prepareStatement(s"SELECT * From standard_users WHERE Username = '$usersUserName' AND password = '$usersPassword'")
    var validUsername = statement.executeQuery()

    try {
      if (!validUsername.next()) {
        println("Username and Password are incorrect")
        false

      } else {
        true
      }
    }
  }

  def validAdmLogin(usersPassword: String): Boolean = {
    connect()
    val statement = connection.prepareStatement(s"SELECT * From admin_users WHERE Username = 'Admin' AND password = '$usersPassword'")
    var validUsername = statement.executeQuery()

    try {
      if (!validUsername.next()) {
        println("Password incorrect")
        false

      } else {
        true
      }
    }
  }

  def disDB(): Unit = {

    connection.close()

  }




}
