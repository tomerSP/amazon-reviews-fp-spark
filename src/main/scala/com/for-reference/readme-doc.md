# Amazon Reviews Functional Programming with Spark

## Project Overview

This project demonstrates functional programming principles using Apache Spark to analyze Amazon product reviews. The implementation emphasizes pure functions, immutability, and functional composition while leveraging Spark's distributed computing capabilities.

## Key Features Demonstrated

### Functional Core
- **Pure Functions**: All data transformations are implemented as pure functions without side effects
- **Immutable Data Structures**: Uses Scala case classes and immutable collections
- **Function Composition**: Demonstrates composition using custom `pipe` combinator
- **Currying & Partial Application**: Shows partial function application for flexible filtering

### Apache Spark Operations
1. **DataFrame Operations**: GroupBy, aggregations, and joins
2. **RDD Transformations**: Map, filter, groupByKey, mapValues
3. **Window Functions**: Ranking and partitioning
4. **Data I/O**: JSON reading and writing to multiple formats

### Advanced Functional Programming

1. **Custom Combinators**
   - `whenThen`: Conditional transformation combinator
   - `pipe`: Function composition combinator

2. **Closures in Spark**
   - `createQualityFilter`: Creates closures that capture threshold values
   - Demonstrates how Spark serializes and distributes closures

3. **Tail-Recursive Functions**
   - `calculatePercentile`: Tail-recursive percentile calculation
   - `groupConsecutiveReviews`: Groups reviews using tail recursion with accumulator

4. **Pattern Matching**
   - Extensive use with case classes
   - Guard conditions in sentiment classification
   - Safe array destructuring in helpfulness calculation

5. **Functional Error Handling**
   - `Try` monad for safe JSON parsing
   - `Either` type for validation with custom error types
   - Compositional error handling in processing pipeline

## Project Structure

```
amazon-reviews-fp-spark/
├── src/
│   ├── main/
│   │   └── scala/
│   │       └── com/reviews/analysis/
│   │           └── AmazonReviewsAnalysis.scala
│   └── test/
│       └── scala/
│           └── com/reviews/analysis/
│               └── AmazonReviewsAnalysisTest.scala
├── build.sbt
├── project/
│   └── plugins.sbt
└── README.md
```

## Data Model

### Input: Review (from JSON)
- `reviewerID`: Unique reviewer identifier
- `asin`: Product ID (Amazon Standard Identification Number)
- `reviewerName`: Optional reviewer name
- `helpful`: Array of [helpful_votes, total_votes]
- `reviewText`: Full review text
- `overall`: Rating (1-5 scale)
- `summary`: Review summary
- `unixReviewTime`: Timestamp of review

### Processed Data Structures
- `ProcessedReview`: Enriched review with computed fields
- `ProductStats`: Aggregated product statistics
- `ReviewerProfile`: Reviewer behavior analysis

## Key Algorithms

### Helpfulness Ratio Calculation
```scala
val calculateHelpfulness: Array[Int] => Double = {
  case Array(helpful, total) if total > 0 => helpful.toDouble / total
  case _ => 0.0
}
```

### Quality Filtering with Closures
```scala
def createQualityFilter(minRating: Double, minHelpfulness: Double) = {
  (review: ProcessedReview) => 
    review.rating >= minRating && 
    review.helpfulnessRatio >= minHelpfulness
}
```

### Function Composition Pipeline
```scala
val summaryPipeline = pipe(
  filterByMinRating(4.0) _,
  whenThen((r: ProcessedReview) => r.reviewLength > 100)(identity)
)
```

## Running the Application

### Prerequisites
- Scala 2.12.17
- Apache Spark 3.4.0
- sbt 1.8.0+

### Build
```bash
sbt clean compile
```

### Run Tests
```bash
sbt test
```

### Execute Main Application
```bash
sbt "run input/reviews.json output/"
```

Or with assembled JAR:
```bash
sbt assembly
spark-submit --class com.reviews.analysis.AmazonReviewsAnalysis \
  target/scala-2.12/amazon-reviews-fp-spark-assembly-1.0.0.jar \
  input/reviews.json output/
```

## Sample Data Format

Input JSON format:
```json
{
  "reviewerID": "A2SUAM1J3GNN3B",
  "asin": "0000013714",
  "reviewerName": "J. McDonald",
  "helpful": [2, 3],
  "reviewText": "I bought this for my husband...",
  "overall": 5.0,
  "summary": "Five Stars",
  "unixReviewTime": 1252800000
}
```

## Output Files

The application generates three output directories:
1. `output/product_stats/` - Product-level aggregations (JSON)
2. `output/ranked_reviews/` - Top helpful reviews per year (Parquet)
3. `output/reviewer_profiles/` - Reviewer behavior analysis (JSON)

## Testing Strategy

Tests are separated into categories:
- **Pure Function Tests**: Test functional core in isolation
- **Combinator Tests**: Verify custom combinators
- **Currying Tests**: Test partial application
- **Tail Recursion Tests**: Verify recursive functions
- **Error Handling Tests**: Test Try/Either monads

## Performance Considerations

- Uses DataFrame caching for repeated operations
- Implements tail recursion for stack-safe operations
- Leverages Spark's lazy evaluation for optimization
- Partitions data by year for efficient window operations

## Design Principles

1. **Separation of Concerns**: Pure logic separated from I/O operations
2. **Referential Transparency**: All pure functions are referentially transparent
3. **Composability**: Small functions composed into complex transformations
4. **Type Safety**: Extensive use of Scala's type system
5. **Immutability**: No mutable state throughout the application

## Future Enhancements

- Add streaming support with Spark Structured Streaming
- Implement more sophisticated sentiment analysis
- Add configuration management with pureconfig
- Extend error handling with cats-effect IO monad
- Add property-based testing with ScalaCheck