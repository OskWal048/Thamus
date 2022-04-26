import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.neo4j.driver.*
import java.io.File
import java.time.Instant
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.math.pow

var inputFileName = "simulatorInputData.json"
var outputFileName = "outputData"
var inputData: InputData? = null
var dbWriter: DBWriter = DBWriter()

var numberOfDays = 100
var numberOfPatientsZero = 1
var probabilityOfInfection = 0.01
var probabilityOfInfectionRange: ProbabilityRange = ProbabilityRange(0.005, 0.25)

var fearFactor = 5.0
var probabilityAsymptomatic = 0.405
var vaccineImmunity = 0.84
var naturalImmunity = 0.805
var naturalImmunityDays = 182
var vaccineData: Map<String, Int> = HashMap()

var savedData: MutableList<SimulationDay> = ArrayList(numberOfDays+1)

val json = Json { ignoreUnknownKeys = true; prettyPrint = true}

var rs: MutableMap<Int, Double> = HashMap()

enum class Compartment{
    SUSCEPTIBLE,
    EXPOSED,
    INFECTED,
    RECOVERED,
    DEAD
}

@Serializable
data class InputData(var numberOfDays: Int? = null,
                     var numberOfPatientsZero: Int? = null,
                     var probabilityOfInfectionRange: ProbabilityRange? = null,
                     var fearFactor: Double? = null,
                     var probabilityAsymptomatic: Double? = null,
                     var vaccineImmunity: Double? = null,
                     var naturalImmunity: Double? = null,
                     var naturalImmunityDays: Int? = null,
                     var vaccineData: Map<String, Int>? = null)

@Serializable
data class OutputData(var simulationResults: List<SimulationDay>)

@Serializable
data class SimulationDay(var dayNumber: Int,
                         var compartmentPopulations: Map<Compartment, Int>)

@Serializable
data class ProbabilityRange(var lowerBound: Double, var upperBound: Double)

class DBWriter :
    AutoCloseable {

    private val uri = "bolt://localhost:7687"
    private val user = "thamus"
    private val password = "thamus"
    private val driver: Driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
    private val session = driver.session()
//    private val session = driver.session(SessionConfig.forDatabase("thamus"))

    @Throws(Exception::class)
    override fun close() {
        driver.close()
    }

    fun makeEveryoneSusceptible(){
        val queryString = "MATCH(n:Person) SET n.compartment = \"${Compartment.SUSCEPTIBLE}\" " +
                            "SET n.daysExposed = 0 SET n.daysInfected = 0 SET n.daysInfecting = 0 " +
                            "SET n.vaccinated = False SET n.naturalImmunity = False " +
                            "SET n.totalDaysExposed = 0 SET n.totalDaysInfected = 0 SET n.totalDaysInfecting = 0 " +
                            "SET n.willDie = False SET n.daysNaturalImmunity = 0 SET n.r = 0"
        session.writeTransaction{ tx ->
            tx.run(queryString)
        }
    }

    fun updatePersonsCompartment(id: Int, compartment: Compartment){
        val queryString = "MATCH(n:Person) WHERE id(n) = $id SET n.compartment = \"${compartment}\""
        session.writeTransaction{ tx ->
            tx.run(queryString)
        }
    }

    fun vaccinatePerson(id: Int){
        val queryString = "MATCH(n:Person) WHERE id(n) = $id " +
                            "SET n.vaccinated = True "
        session.writeTransaction{ tx ->
            tx.run(queryString)
        }
    }

    fun getAverageR(): Double{
        var avgR = 0.0

        val queryString = "MATCH(n:Person) WHERE n.daysInfecting > n.totalDaysInfecting AND n.r > 0 RETURN avg(n.r) "
        session.writeTransaction{ tx ->
            val result: Result = tx.run(queryString)
            val res = result.single()["avg(n.r)"]
            if(res != null && res.toString().lowercase(Locale.getDefault()) != "null")
                avgR = res.asDouble()
        }

        return avgR
    }

    fun makePatientZero(id: Int){
        val daysInfecting = (0..4).random()
        val totalDaysInfectedString = "round(rand()*21+7)"
        val totalDaysInfectingString = "CASE WHEN n.preexistingCondition = False THEN round(rand()*5+5) ELSE round(rand()*15+5) END"
        val willDieString = "CASE WHEN rand()*100 <= 10^(-3.27 + 0.0524*n.age) \n" +
                                "THEN True \n" +
                                "ELSE False \n" +
                            "END \n"


        val queryString = "MATCH(n:Person) WHERE id(n) = $id " +
                            "SET n.compartment = \"${Compartment.INFECTED}\" " +
                            "SET n.daysInfecting = $daysInfecting " +
                            "SET n.daysInfected = ${(0 .. daysInfecting).random()} " +
                            "SET n.totalDaysInfected = $totalDaysInfectedString \n" +
                            "SET n.totalDaysInfecting = $totalDaysInfectingString \n" +
                            "SET n.willDie = $willDieString "

        session.writeTransaction{ tx ->
            tx.run(queryString)
        }
    }

    fun turnExposed(){
        val queryString = "MATCH(n:Person{compartment:\"${Compartment.EXPOSED}\"}) " +
                            "WHERE n.daysExposed = n.totalDaysExposed OR n.daysExposed >= 14 " +
                            "SET n.compartment = CASE WHEN n.asymptomatic = True THEN \"${Compartment.SUSCEPTIBLE}\" " +
                                "ELSE \"${Compartment.INFECTED}\" END " +
                            "SET n.daysExposed = 0 SET n.daysInfected = 0 " +
                            "SET n.totalDaysExposed = 0 SET n.r = 0"

        session.writeTransaction{ tx ->
            tx.run(queryString)
        }
    }

    fun turnInfected(){
        val queryString = "MATCH(n:Person{compartment:\"${Compartment.INFECTED}\"}) " +
                "WHERE n.daysInfected = n.totalDaysInfected OR n.daysInfected >= 28 " +
                "SET n.compartment = CASE WHEN n.willDie = True THEN \"${Compartment.DEAD}\" " +
                    "ELSE \"${Compartment.RECOVERED}\" END " +
                "SET n.naturalImmunity = True SET n.daysNaturalImmunity = 0 " +
                "SET n.daysExposed = 0 SET n.daysInfected = 0 SET n.daysInfecting = 0 " +
                "SET n.totalDaysExposed = 0 SET n.r = 0"

        session.writeTransaction{ tx ->
            tx.run(queryString)
        }
    }

    fun wearOffNaturalImmunity(){
        val queryString = "MATCH(n:Person) " +
                            "WHERE n.daysNaturalImmunity >= $naturalImmunityDays " +
                            "SET n.naturalImmunity = False " +
                            "SET n.naturalImmunityDays = 0 "

        session.writeTransaction{ tx ->
            tx.run(queryString)
        }
    }

    fun infect(){
        val willBeAsymptomaticString = "CASE WHEN rand() <= $probabilityAsymptomatic THEN True ELSE False END"
        val totalDaysExposedString = "round(rand()*12+2)"
        val totalDaysInfectedString = "round(rand()*21+7)"
        val totalDaysInfectingString = "CASE WHEN other.preexistingCondition = False THEN round(rand()*5+5) ELSE round(rand()*15+5) END"
        val willDieString = "CASE WHEN other.willBeAsymptomatic = True \n" +
                                "THEN False \n" +
                                "ELSE \n" +
                                    "CASE WHEN rand()*100 <= 10^(-3.27 + 0.0524*other.age) \n" +
                                        "THEN True \n" +
                                        "ELSE False \n" +
                                    "END \n" +
                            "END "

        val queryString = "MATCH(inf:Person) - [r:KNOWS] - (other:Person) " +
                            "WHERE (inf.compartment = \"${Compartment.INFECTED}\" OR " +
                                "(inf.compartment = \"${Compartment.EXPOSED}\" AND inf.totalDaysExposed - inf.daysExposed <= 3)) AND \n" +
                            "inf.daysInfecting <= inf.totalDaysInfecting AND \n" +
                            "(other.compartment = \"${Compartment.SUSCEPTIBLE}\" OR other.compartment = \"${Compartment.RECOVERED}\") AND \n" +
                            "rand() <= $probabilityOfInfection AND \n" +
                            "((other.vaccinated = True AND rand() >= ${vaccineImmunity}) OR other.vaccinated = False) AND " +
                            "((other.naturalImmunity = True AND rand() >= ${naturalImmunity}) OR other.naturalImmunity = False) \n" +
                            "SET other.compartment = \"${Compartment.EXPOSED}\" \n" +
                            "SET other.willBeAsymptomatic = " + willBeAsymptomaticString + " \n" +
                            "SET other.daysInfected = 0 " +
                            "SET other.daysInfecting = 0 " +
                            "SET other.daysExposed = 0 \n" +
                            "SET other.totalDaysExposed = $totalDaysExposedString \n" +
                            "SET other.totalDaysInfected = $totalDaysInfectedString \n" +
                            "SET other.totalDaysInfecting = $totalDaysInfectingString \n" +
                            "SET other.willDie = $willDieString " +
                            "SET inf.r = inf.r + 1"

        session.writeTransaction{ tx ->
            tx.run(queryString)
        }
    }

    fun incrementDayCounters(){
        val queryString = "MATCH(n:Person{compartment:\"${Compartment.EXPOSED}\"}) " +
                            "SET n.daysExposed = n.daysExposed + 1 " +
                            "SET n.daysInfecting = CASE WHEN n.totalDaysExposed - n.daysExposed <= 3 THEN n.daysInfecting + 1 ELSE n.daysInfecting END"

        val queryString2 = "MATCH(n:Person{compartment:\"${Compartment.INFECTED}\"}) " +
                            "SET n.daysInfected = n.daysInfected + 1 " +
                            "SET n.daysInfecting = CASE WHEN n.daysInfecting <= n.totalDaysInfecting THEN n.daysInfecting + 1 ELSE n.daysInfecting END"

        val queryString3 = "MATCH(n:Person) " +
                            "WHERE n.naturalImmunity = True " +
                            "SET n.daysNaturalImmunity = n.daysNaturalImmunity + 1 "

        session.writeTransaction{ tx ->
            tx.run(queryString)
            tx.run(queryString2)
            tx.run(queryString3)
        }
    }

    fun getInfectedToAllRatio(): Double{
        var ratio = 0.1

        val queryString1 = "MATCH(n:Person{compartment:\"${Compartment.INFECTED}\"}) RETURN count(n)"
        val queryString2 = "MATCH(n:Person) RETURN count(n)"

        session.writeTransaction{ tx ->
            val result1: Result = tx.run(queryString1)
            val result2: Result = tx.run(queryString2)

            ratio = result1.single()[0].asDouble()/result2.single()[0].asDouble()
        }

        return ratio
    }

    fun getRandomNodes(number: Int): List<Int>{

        var queryResultList: MutableList<Record> = ArrayList(number)
        var randomNodeIds: MutableList<Int> = ArrayList(number)

        val queryString = "MATCH(n:Person) RETURN id(n) ORDER BY rand() LIMIT $number"
        session.writeTransaction{ tx ->
            val result: Result = tx.run(queryString)
            queryResultList = result.list()
        }

        for(record in queryResultList)
            randomNodeIds.add(record["id(n)"].asInt())

        return randomNodeIds
    }

    fun getRandomUnvaccinatedNodes(number: Int): List<Int>{

        var queryResultList: MutableList<Record> = ArrayList(number)
        var randomNodeIds: MutableList<Int> = ArrayList(number)

        val queryString = "MATCH(n:Person{vaccinated: False}) RETURN id(n) ORDER BY rand() LIMIT $number"
        session.writeTransaction{ tx ->
            val result: Result = tx.run(queryString)
            queryResultList = result.list()
        }

        for(record in queryResultList)
            randomNodeIds.add(record["id(n)"].asInt())

        return randomNodeIds
    }

    fun getCompartmentSizes(): Map<Compartment, Int>{

        var record: Record
        var sizesMap: MutableMap<Compartment, Int> = HashMap(Compartment.values().size)

        val queryString = "MATCH(n:Person)\n" +
                            "WITH\n" +
                            "    CASE WHEN (n.compartment = \"${Compartment.SUSCEPTIBLE}\") THEN 1 ELSE 0 END as sus,\n" +
                            "    CASE WHEN (n.compartment = \"${Compartment.EXPOSED}\") THEN 1 ELSE 0 END as exp,\n" +
                            "    CASE WHEN (n.compartment = \"${Compartment.INFECTED}\") THEN 1 ELSE 0 END as inf,\n" +
                            "    CASE WHEN (n.compartment = \"${Compartment.RECOVERED}\") THEN 1 ELSE 0 END as rec,\n" +
                            "    CASE WHEN (n.compartment = \"${Compartment.DEAD}\") THEN 1 ELSE 0 END as dea\n" +
                            "RETURN sum(sus) AS SUSCEPTIBLE, sum(exp) AS EXPOSED, sum(inf) AS INFECTED, sum(rec) AS RECOVERED, sum(dea) AS DEAD"
        session.writeTransaction{ tx ->
            val result: Result = tx.run(queryString)
            record = result.single()

            sizesMap[Compartment.SUSCEPTIBLE] = record["SUSCEPTIBLE"].asInt()
            sizesMap[Compartment.EXPOSED] = record["EXPOSED"].asInt()
            sizesMap[Compartment.INFECTED] = record["INFECTED"].asInt()
            sizesMap[Compartment.RECOVERED] = record["RECOVERED"].asInt()
            sizesMap[Compartment.DEAD] = record["DEAD"].asInt()
        }

        return sizesMap
    }
}

fun changeCompartments(){
    dbWriter.turnExposed()
    dbWriter.turnInfected()
}

fun vaccinate(dayNumber: Int){
    if(vaccineData.keys.contains(dayNumber.toString())){
        val numberVaccinated = vaccineData[dayNumber.toString()]!!

        val randomIds = dbWriter.getRandomUnvaccinatedNodes(numberVaccinated)
        for(id in randomIds)
            dbWriter.vaccinatePerson(id)
    }
}

fun simulationStep(){
    dbWriter.incrementDayCounters()
    dbWriter.infect()
    changeCompartments()
    dbWriter.wearOffNaturalImmunity()
    val ratioReverse = 1 - dbWriter.getInfectedToAllRatio()
    probabilityOfInfection = ratioReverse.pow(fearFactor) * (probabilityOfInfectionRange.upperBound - probabilityOfInfectionRange.lowerBound) + probabilityOfInfectionRange.lowerBound
}

fun readInputData(){
    if(inputFileName == ""){
        inputData = InputData(numberOfDays, numberOfPatientsZero, probabilityOfInfectionRange, fearFactor,
                                probabilityAsymptomatic, vaccineImmunity, naturalImmunity, naturalImmunityDays, HashMap())
        return
    }

    val inputString = File(inputFileName).readText()

    inputData = json.decodeFromString<InputData>(inputString)
    if(inputData!!.numberOfDays != null)
        numberOfDays = inputData!!.numberOfDays!!
    if(inputData!!.numberOfPatientsZero != null)
        numberOfPatientsZero = inputData!!.numberOfPatientsZero!!
    if(inputData!!.probabilityOfInfectionRange != null)
        probabilityOfInfectionRange = inputData!!.probabilityOfInfectionRange!!
    if(inputData!!.fearFactor != null)
        fearFactor = inputData!!.fearFactor!!
    if(inputData!!.probabilityAsymptomatic != null)
        probabilityAsymptomatic = inputData!!.probabilityAsymptomatic!!
    if(inputData!!.vaccineImmunity != null)
        vaccineImmunity = inputData!!.vaccineImmunity!!
    if(inputData!!.numberOfPatientsZero != null)
        naturalImmunity = inputData!!.naturalImmunity!!
    if(inputData!!.numberOfPatientsZero != null)
        naturalImmunityDays = inputData!!.naturalImmunityDays!!
    if(inputData!!.vaccineData != null)
        vaccineData = inputData!!.vaccineData!!
}

fun simulationSetup(){
    dbWriter.makeEveryoneSusceptible()

    for(id in dbWriter.getRandomNodes(numberOfPatientsZero))
        dbWriter.makePatientZero(id)

    probabilityOfInfection = probabilityOfInfectionRange.upperBound

}

fun rememberData(dayNumber: Int){
    savedData.add(SimulationDay(dayNumber, dbWriter.getCompartmentSizes()))
}

fun writeToCsvFile(){
    val file = File("$outputFileName.csv")
    file.writeText("DayNumber,Susceptible,Exposed,Infected,Recovered,Dead,R\n")
    for(day in savedData)
        file.appendText(day.dayNumber.toString() + ',' +
                                day.compartmentPopulations[Compartment.SUSCEPTIBLE] + ',' +
                                day.compartmentPopulations[Compartment.EXPOSED] + ',' +
                                day.compartmentPopulations[Compartment.INFECTED] + ',' +
                                day.compartmentPopulations[Compartment.RECOVERED] + ',' +
                                day.compartmentPopulations[Compartment.DEAD] + "," +
                                rs[day.dayNumber] + "\n")

}

fun saveDataToFile(){
    var data = OutputData(savedData)
    File("$outputFileName.json").writeText(json.encodeToString(data))
    writeToCsvFile()
}

fun main(args: Array<String>) {
    val startTime = Instant.now().epochSecond

    if(args.isNotEmpty()){
        if (args.contains("-h") || args.contains("-help")
            || args.contains("--h") || args.contains("--help")
            || args.contains("-?") || args.contains("--?") || args.contains("?")
        ) {
            println("Thamus COVID-19 Simulator help: ")
            println(" ")
            println("Run program with no parameters if input data is in file inputData.json in the same directory as the program")
            println(" ")
            println("Otherwise, you can use these arguments (note that input file overrides command line arguments): ")
            println("  - Help: -h")
            println("  - Specify other input file path and name (must be a JSON file): -f [path/name]")
            println("  - Use only default arguments, no input file: -nof")
            println("")
            println("There also need to be a Neo4j database set up on port 7687 with user thamus and password thamus")
            println("")
            return
        }
        if (args.contains("-f")) {
            val index = args.indexOf("-f")
            if (index + 1 < args.size && args[index + 1][0] != '-')
                inputFileName = args[index + 1]
        }

        if (args.contains(("-nof"))) {
            inputFileName = ""
        }
    }

    readInputData()
    simulationSetup()

    rememberData(0)
    rs[0] = 0.0
    for(i in 1 .. numberOfDays){
        println("day $i of simulation")

        simulationStep()

        vaccinate(i)

        rememberData(i)
        val r = dbWriter.getAverageR()
            rs[i] = r
    }

    saveDataToFile()
    println("average R: ${rs.values.stream().mapToDouble{num -> num}.average().asDouble}")
    val endTime = Instant.now().epochSecond
    println("execution time: " + (endTime - startTime) + "s")
}