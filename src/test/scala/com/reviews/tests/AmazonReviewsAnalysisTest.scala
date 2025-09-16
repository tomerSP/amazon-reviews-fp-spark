package com.reviews.tests

import com.reviews.analysis.AmazonReviewsAnalysis._
import com.reviews.analysis.{ProcessedReview, ProductStats, Review}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class AmazonReviewsAnalysisTest extends AnyFunSuite {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("AmazonReviewsAnalysisTest")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // -------------- PURE FUNCTIONS --------------

    // getHelpfulnessVotes
    test("getHelpfulnessVotes should correctly parse helpfulness string") {
        val result = getHelpfulnessVotes(Option("10"))
        assert(result == 10)
    }

    test("getHelpfulnessVotes should return None for invalid input") {
        val result = getHelpfulnessVotes(None)
        assert(result == 0)
    }

    // determineSentiment
    test("determineSentiment should correctly classify positive sentiment") {
        val result = determineSentiment(5.0)
        assert(result == "positive")
    }

    test("determineSentiment should correctly classify negative sentiment") {
        val result = determineSentiment(1.0)
        assert(result == "negative")
    }

    test("determineSentiment should correctly classify neutral sentiment") {
        val result = determineSentiment(3.0)
        assert(result == "neutral")
    }

    // extractYear
    test("extractYear should correctly parse a date string") {
        val result = extractYear(1757964116)
        assert(result == 2025)
    }

    // -------------- CUSTOM COMBINATOR --------------

    // whenThen
    test("whenThen should apply the 'then' function if the 'when' predicate is true") {
        val isEven: Int => Boolean = _ % 2 == 0
        val double: Int => Int = _ * 2
        val result = whenThen(isEven)(double)(4)
        assert(result == 8)
    }

    test("whenThen should return the original value if the 'when' predicate is false") {
        val isEven: Int => Boolean = _ % 2 == 0
        val double: Int => Int = _ * 2
        val result = whenThen(isEven)(double)(3)
        assert(result == 3)
    }

    // -------------- CURRIED FUNCTIONS --------------

    test("filterByMinRating should correctly filter a ProcessedReview object") {
        // Define a minimum rating threshold.
        val minRating = 4.0
        // Create an instance of the curried function.
        val filter = filterByMinRating(minRating)_

        // Create test data.
        val reviewAboveMin = ProcessedReview("id1", "prod1", 5.0, 10, 20, "text", 2022)
        val reviewAtMin = ProcessedReview("id2", "prod2", 4.0, 5, 10, "text", 2023)
        val reviewBelowMin = ProcessedReview("id3", "prod3", 3.0, 2, 5, "text", 2021)

        // Apply the filter function and check the results.
        // The filter function should return true for reviews with ratings >= minRating.
        assert(filter(reviewAboveMin))
        assert(filter(reviewAtMin))
        assert(!filter(reviewBelowMin))
    }

    // filterByMinLength
    test("filterByMinLength should correctly filter a ProcessedReview object based on review length") {
        // Define a minimum review length threshold.
        val minLength = 10
        // Create an instance of the curried function.
        val filter = filterByMinLength(minLength)_

        // Create test data.
        val reviewLongEnough = ProcessedReview("id1", "prod1", 5.0, 10, 20, "This is a long review text.", 2022)
        val reviewExactlyEnough = ProcessedReview("id2", "prod2", 4.0, 5, 10, "1234567890", 2023)
        val reviewTooShort = ProcessedReview("id3", "prod3", 3.0, 2, 5, "short", 2021)

        // Apply the filter function and check the results.
        assert(filter(reviewLongEnough))
        assert(filter(reviewExactlyEnough))
        assert(!filter(reviewTooShort))
    }

    // -------------- CLOSURES IN SPARK TRANSFORMATIONS --------------

    // createQualityFilter
    test("createQualityFilter should return true for reviews meeting thresholds") {
        val filter = createQualityFilter(minRating = 3.0, minHelpfulness = 5)

        val goodReview = ProcessedReview(
            reviewerID = "user1",
            productID = "prod1",
            rating = 4.0,
            helpfulnessVotes = 6,
            reviewLength = 100,
            sentiment = "positive",
            year = 2022
        )

        assert(filter(goodReview))
    }

    test("createQualityFilter should return false for reviews below rating threshold") {
        val filter = createQualityFilter(minRating = 3.0, minHelpfulness = 5)

        val lowRatingReview = ProcessedReview(
            reviewerID = "user2",
            productID = "prod2",
            rating = 2.5,
            helpfulnessVotes = 10,
            reviewLength = 80,
            sentiment = "neutral",
            year = 2022
        )

        assert(!filter(lowRatingReview))
    }

    test("createQualityFilter should return false for reviews below helpfulness threshold") {
        val filter = createQualityFilter(minRating = 3.0, minHelpfulness = 5)

        val lowHelpfulnessReview = ProcessedReview(
            reviewerID = "user3",
            productID = "prod3",
            rating = 4.5,
            helpfulnessVotes = 2,
            reviewLength = 50,
            sentiment = "positive",
            year = 2022
        )

        assert(!filter(lowHelpfulnessReview))
    }


    // -------------- FUNCTIONAL ERROR HANDLING --------------

    test("validateAndProcess should return Right for a valid review") {
        val review = Review(
            reviewerID = "user1",
            asin = "prod1",
            reviewerName = Some("Alice"),
            vote = Some("12"),
            reviewText = Some("Great product!"),
            overall = 5.0,
            summary = Some("Loved it"),
            unixReviewTime = 1609459200L, // 2021-01-01
            reviewTime = "01 01, 2021",
            image = None,
            verified = Some(true)
        )

        val result = validateAndProcess(review)
        assert(result.isRight)
        assert(result.right.get.reviewerID == "user1")
        assert(result.right.get.helpfulnessVotes == 12)
        assert(result.right.get.sentiment == "positive")
    }

    test("validateAndProcess should return Left when review text is missing") {
        val review = Review(
            reviewerID = "user2",
            asin = "prod2",
            reviewerName = Some("Bob"),
            vote = None,
            reviewText = None,
            overall = 4.0,
            summary = Some("Ok"),
            unixReviewTime = 1609459200L,
            reviewTime = "01 01, 2021",
            image = None,
            verified = Some(false)
        )

        val result = validateAndProcess(review)
        assert(result.isLeft)
        assert(result.left.get.isInstanceOf[InvalidData])
        assert(result.left.get.asInstanceOf[InvalidData].message.contains("Review text is empty"))
    }

    test("validateAndProcess should return Left when rating is invalid") {
        val review = Review(
            reviewerID = "user3",
            asin = "prod3",
            reviewerName = None,
            vote = Some("5"),
            reviewText = Some("Not valid rating"),
            overall = 7.0, // invalid
            summary = None,
            unixReviewTime = 1609459200L,
            reviewTime = "01 01, 2021",
            image = None,
            verified = None
        )

        val result = validateAndProcess(review)
        assert(result.isLeft)
        assert(result.left.get.isInstanceOf[InvalidData])
        assert(result.left.get.asInstanceOf[InvalidData].message.contains("Invalid rating value"))
    }


    // -------------- SPARK TRANSFORMATIONS --------------

    // processReviews
    test("processReviews should return only valid, high-quality processed reviews") {

        val input = Seq(
            // Valid review (rating >= 2.0, helpfulness >= 10)
            Review("u1", "p1", Some("Alice"), Some("15"), Some("Great!"), 4.0,
                Some("summary"), 1609459200L, "01 01, 2021", None, Some(true)),

            // Invalid: review text missing
            Review("u2", "p2", Some("Bob"), Some("20"), None, 4.0,
                Some("summary"), 1609459200L, "01 01, 2021", None, Some(false)),

            // Invalid: rating < 1
            Review("u3", "p3", None, Some("10"), Some("Bad data"), 0.5,
                None, 1609459200L, "01 01, 2021", None, None),

            // Fails quality filter: helpfulness < 10
            Review("u4", "p4", None, Some("5"), Some("Not enough votes"), 5.0,
                None, 1609459200L, "01 01, 2021", None, None),

            // Valid review (boundary case: helpfulness == 10, rating >= 2.0)
            Review("u5", "p5", Some("Carol"), Some("10"), Some("Decent"), 3.0,
                None, 1609459200L, "01 01, 2021", None, None)
        ).toDS()

        val result = processReviews(spark, input).collect().toList

        assert(result.map(_.reviewerID) == List("u1", "u5"))
        assert(result.forall(_.helpfulnessVotes >= 10))
        assert(result.forall(_.rating >= 2.0))
    }

    test("processReviews should produce sentiment correctly") {

        val input = Seq(
            Review("u6", "p6", None, Some("12"), Some("Loved it"), 5.0,
                None, 1609459200L, "01 01, 2021", None, None),
            Review("u7", "p7", None, Some("15"), Some("It was fine"), 3.0,
                None, 1609459200L, "01 01, 2021", None, None),
            Review("u8", "p8", None, Some("20"), Some("Terrible"), 2.0,
                None, 1609459200L, "01 01, 2021", None, None)
        ).toDS()

        val result = processReviews(spark, input).collect()

        val sentiments = result.map(r => (r.reviewerID, r.sentiment)).toMap
        assert(sentiments("u6") == "positive")
        assert(sentiments("u7") == "neutral")
        assert(sentiments("u8") == "negative")
    }

    // generateProductStats
    test("generateProductStats should compute averages and counts correctly") {

        val processed = Seq(
            ProcessedReview("u1", "p1", 4.0, 12, 100, "positive", 2021),
            ProcessedReview("u2", "p1", 2.0, 8, 50, "negative", 2021),
            ProcessedReview("u3", "p2", 5.0, 20, 200, "positive", 2021)
        ).toDS()

        val stats = generateProductStats(spark, processed).collect().toList

        val byProduct = stats.map(s => s.productID -> s).toMap

        val p1Stats = byProduct("p1")
        assert(p1Stats.avgRating == (4.0 + 2.0) / 2.0)
        assert(p1Stats.reviewCount == 2)
        assert(p1Stats.avgHelpfulness == (12.0 + 8.0) / 2.0)

        val p2Stats = byProduct("p2")
        assert(p2Stats.avgRating == 5.0)
        assert(p2Stats.reviewCount == 1)
        assert(p2Stats.avgHelpfulness == 20.0)
    }

    test("generateProductStats should return empty dataset for empty input") {

        val emptyDS = Seq.empty[ProcessedReview].toDS()
        val stats = generateProductStats(spark, emptyDS).collect()

        assert(stats.isEmpty)
    }

    // enrichReviews
    test("enrichReviews should join processed reviews with product stats") {

        val processed = Seq(
            ProcessedReview("u1", "p1", 4.0, 12, 100, "positive", 2021),
            ProcessedReview("u2", "p1", 2.0, 8, 50, "negative", 2021),
            ProcessedReview("u3", "p2", 5.0, 20, 200, "positive", 2021)
        ).toDS()

        val stats = Seq(
            ProductStats("p1", avgRating = 3.0, reviewCount = 2, avgHelpfulness = 10.0),
            ProductStats("p2", avgRating = 5.0, reviewCount = 1, avgHelpfulness = 20.0)
        ).toDS()

        val enriched = enrichReviews(processed, stats).collect().map { r =>
            val reviewerID = r.getAs[String]("reviewerID")
            val productID = r.getAs[String]("productID")
            val avgRating = r.getAs[Double]("productAvgRating")
            val reviewCount = r.getAs[Long]("productReviewCount")
            reviewerID -> (productID, avgRating, reviewCount)
        }.toMap

        val byUser = enriched.toMap

        assert(byUser("u1") == ("p1", 3.0, 2L))
        assert(byUser("u2") == ("p1", 3.0, 2L))
        assert(byUser("u3") == ("p2", 5.0, 1L))
    }

    test("enrichReviews should drop reviews without matching product stats") {
        val processed = Seq(
            ProcessedReview("u4", "p3", 3.0, 10, 40, "neutral", 2021)
        ).toDS()

        val stats = Seq(
            ProductStats("p1", 3.0, 2, 10.0)
        ).toDS()

        val enriched = enrichReviews(processed, stats).collect()
        assert(enriched.isEmpty)
    }


    // rankReviews
    test("rankReviews should assign ranks by helpfulness per year") {
        val enriched = Seq(
            // Year 2021
            ("u1", "p1", 2021, 50),
            ("u2", "p1", 2021, 30),
            ("u3", "p2", 2021, 70),
            // Year 2022
            ("u4", "p3", 2022, 100),
            ("u5", "p4", 2022, 10)
        ).toDF("reviewerID", "productID", "year", "helpfulnessVotes")

        val ranked = rankReviews(enriched).collect().map { r =>
            (r.getAs[String]("reviewerID"), r.getAs[Int]("year"),
              r.getAs[Int]("helpfulnessVotes"), r.getAs[Int]("helpfulnessRank"))
        }.toList

        val byYear = ranked.groupBy(_._2)

        // Year 2021: expect ranks based on helpfulnessVotes (70 -> rank 1, 50 -> rank 2, 30 -> rank 3)
        val year2021 = byYear(2021).map { case (id, _, votes, rank) => id -> (votes, rank) }.toMap
        assert(year2021("u3") == (70, 1))
        assert(year2021("u1") == (50, 2))
        assert(year2021("u2") == (30, 3))

        // Year 2022: expect ranks (100 -> 1, 10 -> 2)
        val year2022 = byYear(2022).map { case (id, _, votes, rank) => id -> (votes, rank) }.toMap
        assert(year2022("u4") == (100, 1))
        assert(year2022("u5") == (10, 2))
    }

    test("rankReviews should keep at most top 100 reviews per year") {

        val manyReviews = (1 to 150).map(i =>
            ("u" + i, "pX", 2021, 200 - i) // helpfulness votes decreasing
        ).toDF("reviewerID", "productID", "year", "helpfulnessVotes")

        val ranked = rankReviews(manyReviews).collect()

        // Should only keep top 100
        assert(ranked.length == 100)

        // Highest votes should have rank 1
        val topReview = ranked.minBy(_.getAs[Int]("helpfulnessRank"))
        assert(topReview.getAs[Int]("helpfulnessRank") == 1)
    }


    // generateReviewerProfiles
    test("generateReviewerProfiles should compute aggregates correctly for each reviewer") {
        val processed = Seq(
            ProcessedReview("u1", "p1", 4.0, 10, 100, "positive", 2021),
            ProcessedReview("u1", "p2", 2.0, 20, 50, "negative", 2021),
            ProcessedReview("u2", "p3", 5.0, 30, 200, "positive", 2021)
        ).toDS()

        val profiles = generateReviewerProfiles(processed).collect().toList
        val byUser = profiles.map(p => p.reviewerID -> p).toMap

        val u1 = byUser("u1")
        assert(u1.totalReviews == 2)
        assert(u1.avgRating == (4.0 + 2.0) / 2.0)
        assert(u1.avgReviewLength == (100.0 + 50.0) / 2.0)
        assert(u1.helpfulnessScore == (10.0 + 20.0) / 2.0)

        val u2 = byUser("u2")
        assert(u2.totalReviews == 1)
        assert(u2.avgRating == 5.0)
        assert(u2.avgReviewLength == 200.0)
        assert(u2.helpfulnessScore == 30.0)
    }

    test("generateReviewerProfiles should return empty RDD for empty input") {
        val emptyDS = Seq.empty[ProcessedReview].toDS()
        val profiles = generateReviewerProfiles(emptyDS).collect()

        assert(profiles.isEmpty)
    }

}