package com.reviews.tests

import com.reviews.analysis.AmazonReviewsPreprocess
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.PrintWriter
import java.nio.file.{Files, Paths}

/**
 * Unit tests for AmazonReviewsPreprocess.
 * Tests data loading and I/O logic separately from core transformations.
 */
class AmazonReviewsPreprocessTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

    // Create a temporary SparkSession for testing
    private var spark: SparkSession = _
    private val testDataPath = "target/test-data.json"

    // Setup: create a temporary SparkSession and a test JSON file before all tests
    override def beforeAll(): Unit = {
        super.beforeAll()
        spark = SparkSession.builder()
          .appName("AmazonReviewsPreprocessTest")
          .master("local[*]")
          .getOrCreate()

        // Create a temporary JSON file for testing
        val jsonContent =
            """
              |{"reviewerID": "A1B2C3D4", "asin": "PROD001", "reviewerName": "Alice", "overall": 5.0, "unixReviewTime": 1609459200, "reviewTime": "01 01, 2021", "reviewText": "Great product!", "vote": "100", "image": [], "summary": "Great", "verified": true}
              |{"reviewerID": "E5F6G7H8", "asin": "PROD002", "reviewerName": "Bob", "overall": 3.0, "unixReviewTime": 1609545600, "reviewTime": "01 02, 2021", "reviewText": "It's okay.", "vote": "5", "image": [], "summary": "OK", "verified": false}
      """.stripMargin

        // Write the JSON content to a file
        val file = new PrintWriter(testDataPath)
        try {
            file.write(jsonContent)
        } finally {
            file.close()
        }
    }

    // Teardown: stop the SparkSession and delete the temporary file after all tests
    override def afterAll(): Unit = {
        super.afterAll()
        if (spark != null) {
            spark.stop()
        }
        Files.deleteIfExists(Paths.get(testDataPath))
    }

    // Test cases for AmazonReviewsPreprocess
    test("loadReviews should return Some(Dataset) for an existing JSON file") {
        val preprocessor = new AmazonReviewsPreprocess(spark)
        val reviewsOption = preprocessor.loadReviews(testDataPath)

        // Verify that a Dataset was successfully returned
        reviewsOption shouldBe a [Some[_]]

        val reviews = reviewsOption.get

        // Verify the count of loaded records
        reviews.count() shouldBe 2

        // Verify the schema matches the Review case class
        val schema = reviews.schema
        schema.fieldNames should contain allOf ("reviewerID", "asin", "overall", "unixReviewTime")
    }

    test("loadReviews should return None for a non-existent file") {
        val preprocessor = new AmazonReviewsPreprocess(spark)
        val nonExistentPath = "non-existent-path.json"

        val reviewsOption = preprocessor.loadReviews(nonExistentPath)

        // Verify that no Dataset was returned
        reviewsOption shouldBe None
    }
}