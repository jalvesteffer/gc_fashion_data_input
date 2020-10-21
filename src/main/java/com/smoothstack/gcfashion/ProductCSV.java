package com.smoothstack.gcfashion;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class ProductCSV implements RequestHandler<S3Event, String> {

	/**
	 * This is a handler method for an AWS Lambda that is triggered when a CSV
	 * product file is uploaded to an S3 bucket
	 */
	public String handleRequest(S3Event event, Context context) {

		// get info for bucket and path containing CSV file to process
		S3EventNotificationRecord record = event.getRecords().get(0);
		String srcBucket = record.getS3().getBucket().getName();
		String srcKey = record.getS3().getObject().getUrlDecodedKey();

		System.out.println("srcBucket: " + srcBucket);
		System.out.println("srcKey: " + srcKey);

		// Download the csv file from AWS S3 bucket
		System.out.println("Getting CSV file from S3...");
		AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
		S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));

		try {
			processCSV(new InputStreamReader(s3Object.getObjectContent()));
		} catch (SQLException e1) {
			System.err.println("SQL Error while processing CSV file");
			return "failure";
		} catch (IOException e2) {
			System.err.println("I/O Error while processing CSV file");
			return "failure";
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e3) {
			System.err.println("Error while processing CSV file");
			return "failure";
		}

		System.out.println("CSV file processing successful");
		return "success";
	}

	/**
	 * This method interprets the contents of a CSV file as products and inserts
	 * them into a database
	 * 
	 * @param inStream the contents of a product CSV file as an input stream
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException
	 */
	private void processCSV(InputStreamReader inStream)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException {

		Connection conn = null; // connect to MySQL DB using JDBC
		String line = ""; // to hold one line of text read from CSV file
		String query = ""; // used to hold insert query

		Class.forName(System.getenv("DB_DRIVER")).newInstance();

		// create a connection
		conn = DriverManager.getConnection(System.getenv("DB_URL"), System.getenv("DB_USERNAME"), System.getenv("DB_PASSWORD"));

		BufferedReader br = new BufferedReader(inStream);

		System.out.println("Begin reading file...");

		for (int x = 0; (line = br.readLine()) != null; x++) {

			// split the read line by the DELIMITER character and store the pieces in array
			// product
			String[] product = line.split(System.getenv("DELIMITER"));

			// display error message and skip line if it does not contain the correct number
			// of columns
			if (product.length != Integer.parseInt(System.getenv("NUM_COL"))) {
				System.err.println("ERROR: csv format : number of columns incorrect on line " + x);
				continue;
			}

			// if the first line is being processed, interpret it as a column names header;
			// otherwise, interpret it as a product record
			if (x == 0) {
				System.out.print("columns ");

				// print all column values for product header
				for (String rec : product) {
					System.out.print(" : " + rec);
				}
				System.out.println();
			} else {
				// product record line
				System.out.print("line " + x);

				// print all column values for product
				for (String rec : product) {
					System.out.print(" : " + rec);
				}
				System.out.println();

				// construct SQL Insert query from product array
				query = "INSERT INTO fashion.product (product_name, gender, description, photo, cat_id, subcat_id, price) "
						+ "VALUES ('" + product[0] + "','" + product[1] + "','" + product[2] + "','" + product[3]
						+ "','" + product[4] + "','" + product[5] + "','" + product[6] + "')";

				System.out.println(query);

				// execute insert statement
				Statement stmt = conn.createStatement();
				stmt.executeUpdate(query);
				System.out.println("Query Executed");
			}
		}

		System.out.println("End of file reached");

		// close BufferedReader
		br.close();

		// close JDBC connection
		conn.close();

	}

}
