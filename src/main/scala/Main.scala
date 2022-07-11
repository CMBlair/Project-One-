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
      println("\u001B24m~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\u001B[0m")
      println("\u001B[33m" + "Welcome to the Pokemon Moves Database");
      println("\u001B24m" + "Please select an option below:");
      println("\u001B24m" + "1: Login");
      println("\u001B24m" + "2: Admin Login");
      println("\u001B24m" + "3: Register");
      println("\u001B24m" + "4: Exit");
      println("\u001B24m~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\u001B[0m")
      val option = scala.io.StdIn.readLine()
      option match {
        case "1" => login()
        //case "2" => adminLogin()
        //case "3" => register()
        case "3" => exit()
        case _ => mainMenu()
      }
    }
    def exit(): Unit = {
      println("\u001B24m~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\u001B[0m")
      println("\u001B[33m" + "Thank you for using the Pokemon Moves Database");
      println("\u001B24m" + "Goodbye!");
      println("\u001B24m~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\u001B[0m")
      System.exit(0)

    }
    def login(): Unit = {
      println("\u001B24m~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\u001B[0m")
      println("\u001B[33m" + "Please enter your username: ");
      val username = scala.io.StdIn.readLine()
      println("\u001B[33m" + "Please enter your password: ");
      val password = scala.io.StdIn.readLine()
      val loggedIn = sql.validateLogin(username, password)
      if(loggedIn) {
        println("\u001B24m" + "Welcome " + username + "!");
        stdMenu()

        }
      }
    def stdMenu(): Unit = {
      println("\u001B24m~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\u001B[0m")
      println("\u001B[33m" + "Please select an option below:");
      println("\u001B24m" + "1: Query the Database");
      println("\u001B24m" + "2: Logout");
      println("\u001B24m" + "3: Change Password");
      println("\u001B24m" + "4: Exit");
      println("\u001B24m~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\u001B[0m")
      val option = scala.io.StdIn.readLine()
      option match {
        case "1" => query()
        case "2" => logout()
        case "3" => changePassword()Mm I 
        case "4" => exit()
        case _ => stdMenu()
      }
    }
    def adminMenu(): Unit = {
    println("\u001B24m--------Admin menu--------\u001B[0m")
    println("\u001B[33mPlease select an option below:")
    println("\u001B24m1: Add a New User")
    println("\u001B24m2: Delete a User")
    println("\u001B24m3: Change Password")
    println("\u001B24m4: Logout")
    println("\u001B24m4: Exit")

    }
  }
}