import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.sys.exit


object Main {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "~/hadoop/hadoop-3.3.3")
    val spark = SparkSession
      .builder
      .appName("PokemonMoves")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")
    sql.connect()
    val dbStr = "jdbc:mysql://localhost:3306/Users"
    var currentUser: String = ""
    spark.read.format("csv")
      .option("header", true)
      .load("C:\\Users\\Corey\\Documents\\untitled\\move-data.csv")
      .write.mode("overwrite").format("jdbc")
      .option("url", dbStr)
      .option("dbtable", "moves")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "corey").save()
    mainMenu()

    def mainMenu(): Unit = {
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("Welcome to the Pokemon Moves Database");
      println("" + "Please select an option below:");
      println("" + "1: Login");
      println("" + "2: Admin Login");
      println("" + "3: Register");
      println("" + "4: Exit");
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      val option = scala.io.StdIn.readLine()
      option match {
        case "1" => login()
        case "2" => adminLogin()
        case "3" => register()
        case "4" => exit()
        case _ => mainMenu()
      }
    }

    def exit(): Unit = {
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("Thank you for using the Pokemon Moves Database");
      println("" + "Goodbye!");
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      System.exit(0)

    }
    def adminLogin(): Unit = {
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("Please enter your password: ");
      val password = scala.io.StdIn.readLine()
      val loggedIn = sql.validAdmLogin(password)
      if (loggedIn) {
        println("" + "Welcome Admin!");
        adminMenu()
      }
      else {
        mainMenu()
      }
    }
    def login(): Unit = {
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("Please enter your username: ");
      val username = scala.io.StdIn.readLine()
      println("Please enter your password: ");
      val password = scala.io.StdIn.readLine()
      val loggedIn = sql.validateLogin(username, password)
      if (loggedIn) {
        println("" + "Welcome " + username + "!");
        stdMenu()
        currentUser = username;
      }
      else {
        mainMenu()
      }
    }
      def stdMenu(): Unit = {
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        println("Please select an option below:");
        println("1: Query the Database");
        println("2: Logout");
        println("3: Change Password");
        println("4: Exit");
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        val option = scala.io.StdIn.readLine()
        option match {
          case "1" => query()
          case "2" => mainMenu()
          case "3" => changePassword()
          case "4" => exit()
          case _ => stdMenu()
        }
      }

      def changePassword(): Unit = {
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        println("What would you like to change your password to")
        val newPassword = scala.io.StdIn.readLine()
        sql.updateStdPassword(currentUser, newPassword)
        stdMenu()
      }
    }
      def changeAdminPassword(): Unit = {
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        println("What would you like to change your password to")
        val newPassword = scala.io.StdIn.readLine()
        sql.updateAdminPassword(newPassword)
        adminMenu()
      }
    def adminMenu(): Unit = {
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("Please select an option below:")
      println("1: Add a New User")
      println("2: Delete a User")
      println("3: Change Password")
      println("4: Logout")
      println("4: Exit")
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      val option = scala.io.StdIn.readLine()
      option match {
        //case "1" => addUser()
        //case "2" => deleteUser()
        //case "3" => changeAdmPassword()
        //case "4" => logout()
        case "5" => exit()
        case _ => adminMenu()
      }
    }

}