import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Main {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "~/hadoop/hadoop-3.3.3")
    val spark = SparkSession
      .builder
      .appName("PokemonMoves")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val url = "jdbc:mysql://localhost:3306/Users"
    val user = "root"
    val pass = "corey"
    val sourceDf=spark.read.format("jdbc").option("url",url)
      .option("dbtable","standard_users").option("user",user)
      .option("password",pass)
      .option("driver", "com.mysql.jdbc.Driver").load()
    sourceDf.show()


    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")

    println("\u001B[35m--------Admin menu--------\u001B[0m")
    println("\u001B[33mPlease select an option 1-8")
    print("Welcome -> ")
    //sql.showUser(currentUser)
    //println("\nUser: "+currentUser)
    println {
      "Option 1: \u001B[35mMake a new Admin\n" +
        "\u001B[33mOption 2: \u001B[35mChange Username\n" +
        "\u001B[33mOption 3: \u001B[35mChange Name\n" +
        "\u001B[33mOption 4: \u001B[35mChange Password\n" +
        "\u001B[33mOption 5: \u001B[35mGo to data \n" +
        "\u001B[33mOption 6: \u001B[35mLogout\n" +
        "\u001B[33mOption 7: \u001B[35mDelete Account\n" +
        "\u001B[33mOption 8: \u001B[35mexit app\u001B[0m"
    }








  }
}