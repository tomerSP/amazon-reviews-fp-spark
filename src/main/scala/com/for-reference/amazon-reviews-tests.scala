package com.reviews.analysis

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import scala.util.{Try, Success, Failure}

/**
 * Unit tests for Amazon Reviews Analysis
 * Tests pure functions separately from Spark operations
 */
class AmazonReviewsAnalysisTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  
  import AmazonReviewsAnalysis._
  
  // ============== TEST DATA ==============
  
  val sampleReview = rawReview(
    reviewerID = "USER123",
    asin = "PROD456",
    reviewerName = Some("John Doe"),
    helpful = Array(10, 15),
    reviewText = "This is a great product. I really enjoyed using it.",
    overall = 4.5,
    summary = "Great product",
    unixReviewTime = 1609459200L // 2021-01-01
  )
  
  val processedReview = ProcessedReview(
    reviewerID = "USER123",
    productID = "PROD456",
    rating = 4.5,
    helpfulnessRatio = 0.67,
    reviewLength = 50,
    sentiment = "positive",
    year = 2021
  )
  
  // ============== PURE FUNCTION TESTS ==============
  
  test("calculateHelpfulness should compute correct ratio") {
    calculateHelpfulness(Array(10, 15)) shouldBe (10.0 / 15.0)
    calculateHelpfulness(Array(0, 10)) shouldBe 0.0
    calculateHelpfulness(Array(5, 5)) shouldBe 1.0
  }
  
  test("calculateHelpfulness should handle edge cases") {
    calculateHelpfulness(Array(10, 0)) shouldBe 0.0  // Division by zero
    calculateHelpfulness(Array()) shouldBe 0.0       // Empty array
    calculateHelpfulness(Array(5)) shouldBe 0.0      // Incomplete array
  }
  
  test("determineSentiment should correctly classify ratings") {
    determineSentiment(5.0) shouldBe "positive"
    determineSentiment(4.0) shouldBe "positive"
    determineSentiment(3.5) shouldBe "neutral"
    determineSentiment(3.0) shouldBe "neutral"
    determineSentiment(2.0) shouldBe "negative"
    determineSentiment(1.0) shouldBe "negative"
  }
  
  test("extractYear should extract correct year from unix timestamp") {
    extractYear(1609459200L) shouldBe 2021  // 2021-01-01
    extractYear(1577836800L) shouldBe 2020  // 2020-01-01
    extractYear(946684800L) shouldBe 2000   // 2000-01-01
  }
  
  // ============== COMBINATOR TESTS ==============
  
  test("whenThen combinator should apply transformation conditionally") {
    val isLongReview = (r: ProcessedReview) => r.reviewLength > 100
    val doubleRating = (r: ProcessedReview) => r.copy(rating = r.rating * 2)
    
    val conditionalTransform = whenThen(isLongReview)(doubleRating)
    
    val shortReview = processedReview.copy(reviewLength = 50)
    val longReview = processedReview.copy(reviewLength = 150)
    
    conditionalTransform(shortReview).rating shouldBe shortReview.rating
    conditionalTransform(longReview).rating shouldBe (longReview.rating * 2)
  }
  
  test("pipe combinator should compose functions correctly") {
    val addOne: Int => Int = _ + 1
    val multiplyByTwo: Int => Int = _ * 2
    
    val composed = pipe(addOne, multiplyByTwo)
    
    composed(5) shouldBe 12  // (5 + 1) * 2 = 12
  }
  
  // ============== CURRIED FUNCTION TESTS ==============
  
  test("filterByMinRating should filter correctly with partial application") {
    val filterHighRatings = filterByMinRating(4.0) _
    
    filterHighRatings(processedReview.copy(rating = 5.0)) shouldBe true
    filterHighRatings(processedReview.copy(rating = 4.0)) shouldBe true
    filterHighRatings(processedReview.copy(rating = 3.5)) shouldBe false
  }
  
  test("filterByMinLength should work with different thresholds") {
    val shortFilter = filterByMinLength(10) _
    val longFilter = filterByMinLength(100) _
    
    val review = processedReview.copy(reviewLength = 50)
    
    shortFilter(review) shouldBe true
    longFilter(review) shouldBe false
  }
  
  // ============== TAIL-RECURSIVE FUNCTION TESTS ==============
  
  test("calculatePercentile should compute correct percentiles") {
    val values = List(1.0, 2.0, 3.0, 4.0, 5.0)
    
    calculatePercentile(values, 0) shouldBe 1.0    // Minimum
    calculatePercentile(values, 50) shouldBe 3.0   // Median
    calculatePercentile(values, 100) shouldBe 5.0  // Maximum
  }
  
  test("calculatePercentile should handle edge cases") {
    calculatePercentile(List.empty, 50) shouldBe 0.0
    calculatePercentile(List(5.0), 50) shouldBe 5.0
  }
  
  test("groupConsecutiveReviews should group reviews by reviewer") {
    val reviews = List(
      processedReview.copy(reviewerID = "A"),
      processedReview.copy(reviewerID = "A"),
      processedReview.copy(reviewerID = "B"),
      processedReview.copy(reviewerID = "A"),
      processedReview.copy(reviewerID = "B"),
      processedReview.copy(reviewerID = "B")
    )
    
    val grouped = groupConsecutiveReviews(reviews)
    
    grouped.length shouldBe 4  // Four groups of consecutive reviews
    grouped(0).forall(_.reviewerID == "A") shouldBe true
    grouped(0).length shouldBe 2
    grouped(1).forall(_.reviewerID == "B") shouldBe true
    grouped(1).length shouldBe 1
  }
  
  // ============== CLOSURE TESTS ==============
  
  test("createQualityFilter should create working closure") {
    val strictFilter = createQualityFilter(4.0, 0.5)
    val lenientFilter = createQualityFilter(2.0, 0.2)
    
    val highQuality = processedReview.copy(rating = 4.5, helpfulnessRatio = 0.8)
    val lowQuality = processedReview.copy(rating = 2.0, helpfulnessRatio = 0.3)
    
    strictFilter(highQuality) shouldBe true
    strictFilter(lowQuality) shouldBe false
    lenientFilter(lowQuality) shouldBe true
  }
  
  // ============== ERROR HANDLING TESTS ==============
  
  test("validateAndProcess should handle valid reviews") {
    val result = validateAndProcess(sampleReview)
    
    result.isRight shouldBe true
    result.getOrElse(fail("Should produce valid result")).productID shouldBe "PROD456"
  }
  
  test("validateAndProcess should reject invalid reviews") {
    val emptyTextReview = sampleReview.copy(reviewText = "")
    val invalidRatingReview = sampleReview.copy(overall = 6.0)
    
    validateAndProcess(emptyTextReview).isLeft shouldBe true
    validateAndProcess(invalidRatingReview).isLeft shouldBe true
  }
  
  test("validateAndProcess should provide appropriate error messages") {
    val emptyTextReview = sampleReview.copy(reviewText = "")
    
    validateAndProcess(emptyTextReview) match {
      case Left(InvalidData(msg)) => msg should include("empty")
      case _ => fail("Should produce InvalidData error")
    }
  }
}