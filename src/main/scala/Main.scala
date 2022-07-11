import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.sys.exit


object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("PokemonMoves")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")
    sql.connect()
    var currentUser: String = ""
    val df = spark.read.format("csv").option("header", "true").load("move-data.csv")

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
    def changeAdmPassword(): Unit = {
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        println("What would you like to change your password to")
        val newPassword = scala.io.StdIn.readLine()
        sql.updateAdmPassword(newPassword)
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
        case "1" => addUser()
        case "2" => deleteUser()
        case "3" => changeAdmPassword()
        case "4" => mainMenu()
        case "5" => exit()
        case _ => adminMenu()
      }
    }
    def register(): Unit = {
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("Please enter your username: ");
      val username = scala.io.StdIn.readLine()
      println("Please enter your password: ");
      val password = scala.io.StdIn.readLine()
      sql.createUser(username, password)
      mainMenu()
    }
    def addUser(): Unit = {
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("Please enter the username: ");
      val username = scala.io.StdIn.readLine()
      println("Please enter the password: ");
      val password = scala.io.StdIn.readLine()
      sql.createUser(username, password)
      adminMenu()
    }
    def deleteUser(): Unit = {
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("Please enter the username: ");
      val username = scala.io.StdIn.readLine()
      sql.deleteUser(username)
      adminMenu()
    }
    def query(): Unit = {
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("Please Choose a Query From the List Below:")
      println("1: Query for a Move")
      println("2: Query for number of moves introduced by generation")
      println("3: Query for number of moves introduced by type")
      println("4: Query for moves by type")
      println("5: Query for number of moves by contest category")
      println("7: Return to Main Menu")
      var option = scala.io.StdIn.readLine()
      option match {
        case "1" => queryMove()
        case "2" => queryGeneration()
        case "3" => queryType()
        case "4" => queryPhysSpec()
        case "5" => queryCategory()
        case "6" => queryShowMovesFromGeneration()
        case "9" => stdMenu()
        case _ => query()
      }
    }
    def queryMove(): Unit = {
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("Please enter the move index 1-728: ");
      val moveIndex = scala.io.StdIn.readLine()
      df.createOrReplaceTempView("moves")
      df.sparkSession.sql("SELECT * FROM moves WHERE Index = " + moveIndex).show()
      spark.catalog.dropTempView("moves")
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      query()
      }
    def queryGeneration(): Unit = {
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("The number of moves introduced by generation: ");
      df.createOrReplaceTempView("moves")
      df.sparkSession.sql("SELECT Generation, COUNT(Generation) AS New_Moves FROM moves GROUP BY Generation ORDER BY Generation ASC").show()
      spark.catalog.dropTempView("moves")
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      query()
    }
    def queryType(): Unit = {
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("The number of moves introduced by type: ");
      df.createOrReplaceTempView("moves")
      df.sparkSession.sql("SELECT Type, COUNT(Type) AS Number_Of_Moves FROM moves GROUP BY Type ORDER BY Type ASC").show()
      spark.catalog.dropTempView("moves")
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      query()
    }
    def queryPhysSpec(): Unit = {
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("Please Pick a type from the List Below")
      df.createOrReplaceTempView("moves")
      df.sparkSession.sql("SELECT DISTINCT Type from moves").show()
      var moveType = scala.io.StdIn.readLine()
      val moveCategory = "Physical"
      val moveCategory2 = "Special"
      df.sparkSession.sql("SELECT Name, Type, Category, PP, Power, Accuracy FROM moves WHERE Type = '" + moveType + "'").show(50)
      spark.catalog.dropTempView("moves")
      query()
    }
    def queryCategory(): Unit =
    {
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("Please Pick a Contest category from the List Below")
      df.sparkSession.sql("SELECT DISTINCT Contest from moves").show()
      var Contest = scala.io.StdIn.readLine()
      df.sparkSession.sql("SELECT Name, Type, Contest FROM moves WHERE Contest = '" + Contest + "'").show(50)
      spark.catalog.dropTempView("moves")
      query()
    }
    def queryShowMovesFromGeneration(): Unit = {
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      println("Please enter the generation 1-7 To show moves from: ");
      val generation = scala.io.StdIn.readLine()
      df.createOrReplaceTempView("moves")
      df.sparkSession.sql("SELECT * FROM moves WHERE Generation = " + generation).show(100)
      spark.catalog.dropTempView("moves")
      println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      query()
    }
  }


  }