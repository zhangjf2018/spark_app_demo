package com.spark.scala

object Match {

  def main(args: Array[String]): Unit = {
    case class Car(brand: String, price: Int)
    val myBYDCar = Car("BYD",89000)
    val myBWMCar = Car("BMW", 1200000)
    val myBenzCar = Car("Benz", 1500000)

    for (car <- List(myBYDCar,myBWMCar,myBenzCar)){
      car match {
          case Car("BYD",89000) => println("BYD")
          case Car("BMW", 1200000) => println("BMW")
          case Car(brand,price) => println("Brand:"+brand+", Price:"+price+", do you want?")
      }
    }

   def plusStep(step: Int) = (value: Int) => value + step
    val plusS = plusStep(3)
    println(plusS(10))

    def count(x: Int) = {
      x + 1
    }
  }
}
