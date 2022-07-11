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
    val df = spark.read.json("hdfs://localhost:9000/user/corey/moves.json")
    df.createOrReplaceTempView("moves")


    def mainMenu(): Unit = {
      println("\u001B[35m~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\u001B[0m")
      println("\u001B[33m" + "Welcome to the Pokemon Moves Database");
      println("\u001B[35m" + "Please select an option below:");
      println("\u001B[35m" + "1: Login");
      println("\u001B[35m" + "2: Admin Login");
      println("\u001B[35m" + "3: Register");
      println("\u001B[35m" + "4: Exit");
      println("\u001B[35m~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\u001B[0m")
      val option = scala.io.StdIn.readLine()
      option match {
        case "1" => login()
        case "2" => adminLogin()
        case "3" => register()
        case "3" => exit()
        case _ => mainMenu()
      }
    }
    def exit(): Unit = {
      println("\u001B[35m~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\u001B[0m")
      println("\u001B[33m" + "Thank you for using the Pokemon Moves Database");
      println("\u001B[35m" + "Goodbye!");
      println("\u001B[35m~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\u001B[0m")
      System.exit(0)

    }
    def login(): Unit = {
      println("\u001B[35m~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\u001B[0m")
      println("\u001B[33m" + "Please enter your username: ");
      val username = scala.io.StdIn.readLine()
      println("\u001B[33m" + "Please enter your password: ");
      val password = scala.io.StdIn.readLine()
      val loggedIn = sql.validateLogin(username, password)
      if(loggedIn) {
        println("\u001B[35m" + "Welcome " + username + "!");


        }
      }
    }
    //df.sparkSession.sql("select Name, Generation from moves where Contest = 'Cool' AND  Category = 'Status'").show()
    println("\u001B[35m--------Admin menu--------\u001B[0m")
    println("\u001B[33mPlease select an option 1-8")
    print("Welcome -> ")
    //sql.showUser(currentUser)
    //println("\nUser: "+currentUser)
    println {
        "\u001B[33mOption 1: \u001B[35mChange Admin Password\n" +
        "\u001B[33mOption 2: \u001B[35mGo to data \n" +
        "\u001B[33mOption 3: \u001B[35mDelete Standard User\n" +
          "\u001B[33mOption 4: \u001B[35mLogout\n" +
        "\u001B[33mOption 5: \u001B[35mexit app\u001B[0m"
    }
  }
}