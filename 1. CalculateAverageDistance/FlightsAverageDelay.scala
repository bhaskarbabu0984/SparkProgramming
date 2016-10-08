//Program - Calculate the average delay by all flights from flight dataset (Flights CSV)

sc

// Data location
val airlinesPath="hdfs:////user/bhaskarbabu09846098/sparkdata/airlines.csv"
val airportsPath="hdfs:///user/bhaskarbabu09846098/sparkdata/airports.csv"
val flightsPath="hdfs:///user/bhaskarbabu09846098/sparkdata/flights.csv"


import org.joda.time._
import org.joda.time.format._
import org.joda.time.LocalTime
import org.joda.time.LocalDate

case class Flight(date: LocalDate,
                  airline: String ,
                  flightnum: String,
                  origin: String ,
                  dest: String ,
                  dep: LocalTime,
                  dep_delay: Double,
                  arv: LocalTime,
                  arv_delay: Double ,
                  airtime: Double ,
                  distance: Double
                   )

def parse(row: String): Flight={

  val fields = row.split(",")
  val datePattern = DateTimeFormat.forPattern("YYYY-mm-dd")
  val timePattern = DateTimeFormat.forPattern("HHmm")

  val date: LocalDate = datePattern.parseDateTime(fields(0)).toLocalDate()
  val airline: String = fields(1)
  val flightnum: String = fields(2)
  val origin: String = fields(3)
  val dest: String = fields(4)
  val dep: LocalTime = timePattern.parseDateTime(fields(5)).toLocalTime()
  val dep_delay: Double = fields(6).toDouble
  val arv: LocalTime = timePattern.parseDateTime(fields(7)).toLocalTime()
  val arv_delay: Double = fields(8).toDouble
  val airtime: Double = fields(9).toDouble
  val distance: Double = fields(10).toDouble
  
  Flight(date,airline,flightnum,origin,dest,dep,
         dep_delay,arv,arv_delay,airtime,distance)
}

//Load flights.csv as RDD 
val flights=sc.textFile(flightsPath)

flights

// The total number of records 
flights.count()

// The first row
flights.first()

//calculate total delays and count using aggregate function
val sumCount=flightsParsed.map(_.dep_delay).aggregate((0.0,0))((acc, value) => (acc._1 + value, acc._2+1),
                                                           (acc1,acc2) => (acc1._1+acc2._1,acc1._2+acc2._2))


//Calculate average Delay
val averageDelay = sumCount._1/sumCount._2

println(averageDelay)