///////////////////////////////////////////////////////////////////////////////////////////////////
// Name:		Mark Barros
// Course:		CS 4350 - Database Systems
// Description:	Demonstration of connecting to a MySQL database using the JDBC connector and
//				performing CRUD operations. Execution on the pomona_transit_system database.

///////////////////////////////////////////////////////////////////////////////////////////////////

// This is the import needed for the Database Operations //////////////////////////////////////////
import java.sql.*;

///////////////////////////////////////////////////////////////////////////////////////////////////
// This controls all the Database Operations. /////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////
public class DatabaseOperations {
	
    // This is a constant that will be used in this class. ----------------------------------------
    private static final int NULL_INTEGER = -1;

    // These are the variables that will be used for all the database queries. --------------------
    static private Connection connection = null;
    static private Statement myStatement = null;
    static private ResultSet myResults = null;
    static private String DBConnection = "jdbc:mysql://localhost:3306/pomona_transit_system";
    static private String Username = "root";
    static private String Password = "l9*K7&g6^D5%";

    // This section contains all the Database Operation's Methods /////////////////////////////////
        
    // 1. Display the schedule of all trips for a given StartLocationName and DestinationName, ----
    //    and Date. In addition to these attributes, the schedule includes: Scheduled StartTime,
    //    ScheduledArrivalTime, DriverID, and BusID.
    public static void DisplayScheduledTrips() {
    	
        String StartLocationName = "Wax Museum";
        String DestinationName = "Ontario International Airport";
        String Date = "2021-11-28";
        
        String currentQuery =
                "SELECT	trip.start_location_name, trip.destination_name, date, " +
                "scheduled_start_time, scheduled_arrival_time, driver_name, bus_id " +
                "FROM trip JOIN trip_offering ON trip.trip_number = trip_offering.trip_number " +
                "WHERE trip.start_location_name = '" + StartLocationName +
                "' AND trip.destination_name = '" + DestinationName +
                "' AND trip_offering.date = '" + Date + "';";

        try {
            connection = DriverManager.getConnection(DBConnection, Username, Password);
            myStatement = connection.createStatement();
            myResults = myStatement.executeQuery(currentQuery);
            
            while(myResults.next()) {
                System.out.print(myResults.getString("start_location_name") + "\t" +
                myResults.getString("destination_name") + "\t" +
                myResults.getDate("date") + "\t" +
                myResults.getTime("scheduled_start_time") + "\t" +
                myResults.getTime("scheduled_arrival_time") + "\t" +
                myResults.getString("driver_name") + "\t" +
                myResults.getInt("bus_id"));
            }
            
            connection.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        
        System.out.println();
        
    }
    
    // 2a. From the Trip Offering table, delete a trip offering specified by Trip #, --------------
    //     Date, and ScheduledStartTime.
    public static void DeleteTripOffering() {
    
	Integer TripNumber = 1;
	String Date = "2021-11-28";
	String ScheduledStartTime = "05:00:00";
	Integer RowsAffected = NULL_INTEGER;
	
	String currentQuery = 
		"DELETE " + 
		"FROM trip_offering " +
    	"WHERE	trip_number =" + TripNumber + " " +
    	"AND date = '" + Date + "' " +
    	"AND scheduled_start_time = '" + ScheduledStartTime + "';";
    	
    try {
        connection = DriverManager.getConnection(DBConnection, Username, Password);
        myStatement = connection.createStatement();
        RowsAffected = myStatement.executeUpdate(currentQuery);
    }
    catch (SQLException e) {
        e.printStackTrace();
    }
    
    System.out.println("Trip Offering Removed: " + RowsAffected + "\n");
	
    }
    
    // 2b. From the Trip Offering table, add a set of trip offerings assuming the values of -------
    //     all attributes are given (the software asks if you have more trips to enter);
    public static void AddTripOffering() {
    
    Integer TripNumber = 5;
    String Date = "2021-11-28";
    String ScheduledStartTime = "09:30:00";
    String ScheduledArrivalTime = "10:15:00";
    String DriverName = "Mick Avery";
    Integer BusID = 400;
    
    String currentQuery =
    		"INSERT INTO trip_offering(trip_number, date, scheduled_start_time, " + 
    				     "scheduled_arrival_time, driver_name, bus_id) " +
    		"VALUES (" + TripNumber + ", '" + Date + "', '" + ScheduledStartTime + "', '" + 
    				 ScheduledArrivalTime + "', '" + DriverName + "', " + BusID + ");";
    
    try {
        connection = DriverManager.getConnection(DBConnection, Username, Password);
        myStatement = connection.createStatement();
        myStatement.executeUpdate(currentQuery);
    }
    catch (SQLException e) {
        e.printStackTrace();
    }
    
    System.out.println("Trip Offering Added.\n");
    
    }

    // 2c. From the Trip Offering table, change the driver for a given Trip offering --------------
    //     (i.e given TripNumber, Date, ScheduledStartTime);
    public static void ChangeDriver() {
    
    	String DriverName = "Tom Johnson";
    	Integer TripNumber = 3;
    	String Date = "2021-11-28";
    	String ScheduledStartTime = "05:00:00";
    	
    	String currentQuery =
    			 "UPDATE trip_offering SET driver_name = '" + DriverName + 
    			 "' WHERE trip_number = " + TripNumber + " AND date = '" + Date + 
    			 "' AND scheduled_start_time = '" + ScheduledStartTime + "';";
    	 
        try {
            connection = DriverManager.getConnection(DBConnection, Username, Password);
            myStatement = connection.createStatement();
            myStatement.executeUpdate(currentQuery);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    	
        System.out.println("Driver Changed.\n");   	 
    
    }
    
    // 2d. From the Trip Offering table, change the bus for a given Trip offering. ----------------
    public static void ChangeBus() {

    	Integer BusID = 400;
    	Integer TripNumber = 2;
    	
    	String currentQuery =
    			"UPDATE trip_offering SET bus_id = " + BusID + 
    			" WHERE trip_number = " + TripNumber + ";";

        try {
            connection = DriverManager.getConnection(DBConnection, Username, Password);
            myStatement = connection.createStatement();
            myStatement.executeUpdate(currentQuery);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    	
        System.out.println("Bus Changed.\n"); 	
    	
    }
    
    // 3. Display the stops of a given trip (i.e. the attributes of the table TripStopInfo). ------
	public static void DisplayStops() {

		Integer TripNumber = 2;
		
		String currentQuery =
				"SELECT stop_number, sequence_number, driving_time " +
				"FROM trip_stop_info " + 
				"WHERE trip_number = " + TripNumber + ";";
		
		System.out.println("The stops for trip number " + TripNumber.toString() + " are: ");
		
	    try {
	        connection = DriverManager.getConnection(DBConnection, Username, Password);
	        myStatement = connection.createStatement();
	        myResults = myStatement.executeQuery(currentQuery);
	        while(myResults.next()) {
	            System.out.println("\t" + myResults.getInt("stop_number") + "\t" + 
	                               myResults.getInt("sequence_number") + "\t" + 
	                               myResults.getInt("driving_time"));
	        }
	        connection.close();
	    }
	    catch (SQLException e) {
	        e.printStackTrace();
	    }
    
	    System.out.println();
	    
    }
    
    // 4. Display the weekly schedule of a given driver and date. ---------------------------------
	public static void DisplayWeeklySchedule() {
	
		String WeekBeginning = "2021-11-28";
		String WeekEnd = "2021-12-04";
		String DriverName = "James Thomas";
		
		String currentQuery =
				"SELECT trip_number, scheduled_start_time, scheduled_arrival_time " +
				" FROM trip_offering " +
				" WHERE (date BETWEEN '" + WeekBeginning + "' AND '" + WeekEnd + "') " + 
				" AND driver_name = '" + DriverName + "';";
		
		System.out.println("The schedule for " + DriverName + " for the week of " + 
						   WeekBeginning + " is:");
		
	    try {
	        connection = DriverManager.getConnection(DBConnection, Username, Password);
	        myStatement = connection.createStatement();
	        myResults = myStatement.executeQuery(currentQuery);
	        
	        while(myResults.next()) {
	            System.out.println("\t" + myResults.getInt("trip_number") + "\t" + 
	            myResults.getString("scheduled_start_time") + "\t" + 
	            myResults.getString("scheduled_arrival_time"));
	        }
	        
	        connection.close();
	    }
	    catch (SQLException e) {
	        e.printStackTrace();
	    }
	    
	    System.out.println();	    
    
	}
    	     
    // 5. Add a driver. ---------------------------------------------------------------------------
	public static void AddDriver() {
		
		String DriverName = "William Jones";
		String DriverTelephoneNumber = "887-987-9094";
		
		String currentQuery =
				"INSERT INTO driver (driver_name, driver_telephone_number) " + 
				"VALUES ('" + DriverName + "', '" + DriverTelephoneNumber + "')";
		
	    try {
	        connection = DriverManager.getConnection(DBConnection, Username, Password);
	        myStatement = connection.createStatement();
	        myStatement.executeUpdate(currentQuery);
	    }
	    catch (SQLException e) {
	        e.printStackTrace();
	    }
	    
	    System.out.println("Driver Added.\n");
		
	}

    // 6. Add a bus. ------------------------------------------------------------------------------
    public static void AddBus() {
    	
    	Integer BusID = 500;
    	String Model = "Eldorado";
    	String Year = "2007";
    	
    	String currentQuery =
    			"INSERT INTO bus(bus_id, model, year) " +
    			"VALUES	(" + BusID + ", '" + Model + "', '" + Year + "')";

	    try {
	        connection = DriverManager.getConnection(DBConnection, Username, Password);
	        myStatement = connection.createStatement();
	        myStatement.executeUpdate(currentQuery);
	    }
	    catch (SQLException e) {
	        e.printStackTrace();
	    }
	    
	    System.out.println("Bus Added.\n");

    }
    
    // 7. Delete a bus. ---------------------------------------------------------------------------
	public static void DeleteBus() {
		
		Integer BusID = 100;
		Integer RowsAffected = NULL_INTEGER;
		
		String currentQuery =
				"DELETE FROM bus WHERE bus_id = " + BusID + ";";
    
	    try {
	        connection = DriverManager.getConnection(DBConnection, Username, Password);
	        myStatement = connection.createStatement();
	        RowsAffected = myStatement.executeUpdate(currentQuery);
	    }
	    catch (SQLException e) {
	        e.printStackTrace();
	    }
	    
	    System.out.println("Bus Removed: " + RowsAffected + "\n");
		
    }

    // 8. Record (insert) the actual data of a given trip offering specified by its key. ----------
    //    The actual data include the attributes of the table ActualTripStopInfo.	
	public static void RecordTripOffering()  {
	
		Integer TripNumber = 4;
		String Date = "2021-11-28";
		String ScheduledStartTime = "09:30:00";
		Integer StopNumber = 2;
		String ScheduledArrivalTime = "10:15:00";
		String ActualStartTime = "09:34:00";
		String ActualArrivalTime = "10:20:00";
		Integer NumberOfPassengersIn = 17;
		Integer NumberOfPassengersOut = 15;
		
		String currentQuery =
				"INSERT INTO actual_trip_stop_info(trip_number, date, scheduled_start_time, " +
				"stop_number, scheduled_arrival_time, actual_start_time, actual_arrival_time, " +
				"number_of_passengers_in, number_of_passengers_out)" +
				" VALUES (" + TripNumber + ", '" + Date + "', '" + ScheduledStartTime + "', " + 
				StopNumber + ", '" + ScheduledArrivalTime + "', '" + ActualStartTime + "', '" + 
				ActualArrivalTime + "', " + NumberOfPassengersIn + ", " + NumberOfPassengersOut + 
				");";
		
	    try {
	        connection = DriverManager.getConnection(DBConnection, Username, Password);
	        myStatement = connection.createStatement();
	        myStatement.executeUpdate(currentQuery);
	    }
	    catch (SQLException e) {
	        e.printStackTrace();
	    }
	    
	    System.out.println("Actual Trip Stop Information Recorded.\n");
	       
	}
    
    // This is the main method. -------------------------------------------------------------------
	public static void main(String[] args) {
		System.out.println("Demo of displaying scheduled trips:");
		DisplayScheduledTrips();
		System.out.println();
		System.out.println("Demo of deleting a trip offering:");
		DeleteTripOffering();
		System.out.println("Demo of adding a trip offering:");
		AddTripOffering();
		System.out.println("Demo of changing a driver:");
		ChangeDriver();
		System.out.println("Demo of changing a bus:");
		ChangeBus();
		System.out.println("Demo of displaying stops:");
		DisplayStops();
		System.out.println("Demo of displaying a weekly schedule:");
		DisplayWeeklySchedule();
		System.out.println("Demo of adding a bus driver:");
		AddDriver();
		System.out.println("Demo of adding a bus:");
		AddBus();
		System.out.println("Demo of deleting a bus:");
		DeleteBus();
		System.out.println("Demo of recording a trip offering:");
		RecordTripOffering();
	}

///////////////////////////////////////////////////////////////////////////////////////////////////
} // End of Database Operations Class. ////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////