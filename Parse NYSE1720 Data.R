# ============================================
# NYSE Data Fetcher and Cleaner
# ============================================

# Install packages if needed
if (!require("quantmod")) install.packages("quantmod")
if (!require("tidyverse")) install.packages("tidyverse")
if (!require("openxlsx")) install.packages("openxlsx")
if (!require("rvest")) install.packages("rvest")
# 
library(quantmod)
library(tidyverse)
library(openxlsx)
library(rvest)

# ============================================
# 1. GET S&P 500 TICKER LIST
# ============================================

cat("Fetching NYSE ticker list...\n")

ticker_df <- read.csv("C:/Users/andre/OneDrive/Desktop/Uni/Machine Learning and Artificial Intelligence in Finance/Project/Portfolio-Asset-Clustering/sampled_tickers.csv")
tickers <- ticker_df$ticker  # Extract the ticker column as a vector

cat(sprintf("Found %d NYSE stocks\n\n", length(tickers)))
ticker_info <- data.frame(
  ticker = tickers
)

# ============================================
# 2. FETCH HISTORICAL DATA FOR ALL STOCKS
# ============================================

# Date range: 2 years
start_date <- "2023-12-1"
end_date <- "2025-12-1"

cat(sprintf("Fetching data from %s to %s\n", start_date, end_date))

# Storage for data
all_data <- list()
failed_tickers <- c()
success_count <- 0

# Progress bar
pb <- txtProgressBar(min = 0, max = length(tickers), style = 3)

for (i in seq_along(tickers)) {
  ticker <- tickers[i]
  
  tryCatch({
    # Fetch data
    data <- getSymbols(ticker, 
                       src = "yahoo", 
                       from = start_date, 
                       to = end_date, 
                       auto.assign = FALSE,
                       warnings = FALSE)
    
    # Convert to dataframe
    df <- data.frame(
      Date = index(data),
      Open = as.numeric(Op(data)),
      High = as.numeric(Hi(data)),
      Low = as.numeric(Lo(data)),
      Close = as.numeric(Cl(data)),
      Volume = as.numeric(Vo(data)),
      Adjusted = as.numeric(Ad(data))
    )
    
    # Add ticker column
    df$Ticker <- ticker
    
    # Clean data
    df <- df %>%
      filter(!is.na(Close), !is.na(Volume)) %>%
      filter(Close > 0, Volume > 0) %>%
      arrange(Date)
    
    # Only keep if we have at least 100 days of data
    if (nrow(df) >= 100) {
      all_data[[ticker]] <- df
      success_count <- success_count + 1
    } else {
      failed_tickers <- c(failed_tickers, paste0(ticker, " (insufficient data)"))
    }
    
    # Small delay to be polite
    Sys.sleep(0.5)
    
  }, error = function(e) {
    failed_tickers <<- c(failed_tickers, paste0(ticker, " (", e$message, ")"))
  })
  
  # Update progress bar
  setTxtProgressBar(pb, i)
}

close(pb)

cat(sprintf("\n\n✓ Successfully fetched data for %d stocks\n", success_count))
cat(sprintf("✗ Failed to fetch %d stocks\n", length(failed_tickers)))

# ============================================
# 3. COMBINE ALL DATA
# ============================================

cat("\nCombining all stock data...\n")

# Combine all dataframes
combined_data <- bind_rows(all_data)

cat(sprintf("Total rows: %s\n", format(nrow(combined_data), big.mark = ",")))
cat(sprintf("Unique stocks: %d\n", length(unique(combined_data$Ticker))))
cat(sprintf("Date range: %s to %s\n", min(combined_data$Date), max(combined_data$Date)))

# ============================================
# 4. DATA QUALITY CHECKS AND CLEANING
# ============================================

cat("\nPerforming data quality checks...\n")

# Remove duplicates
combined_data <- combined_data %>%
  distinct(Ticker, Date, .keep_all = TRUE)

# Check for missing values
missing_summary <- combined_data %>%
  summarise(across(everything(), ~sum(is.na(.))))

cat("Missing values per column:\n")
print(missing_summary)

# Remove rows with any missing critical values
combined_data <- combined_data %>%
  filter(!is.na(Close) & !is.na(Open) & !is.na(High) & !is.na(Low) & !is.na(Volume))

# Check for zero or negative values
combined_data <- combined_data %>%
  filter(Close > 0 & Open > 0 & High > 0 & Low > 0 & Volume >= 0)

# Check for data anomalies (e.g., High < Low)
anomalies <- combined_data %>%
  filter(High < Low | Close > High | Close < Low | Open > High | Open < Low)

if (nrow(anomalies) > 0) {
  cat(sprintf("Found %d anomalous rows, removing...\n", nrow(anomalies)))
  combined_data <- combined_data %>%
    filter(!(High < Low | Close > High | Close < Low | Open > High | Open < Low))
}

# ============================================
# 5. MERGE WITH SECTOR INFORMATION
# ============================================

cat("\nMerging with sector information...\n")

combined_data <- combined_data %>%
  left_join(ticker_info, by = c("Ticker" = "ticker"))

# ============================================
# 6. SAVE TO EXCEL
# ============================================

cat("\nSaving to Excel...\n")

# Create workbook
wb <- createWorkbook()

# Sheet 1: All stock data
addWorksheet(wb, "Stock_Data")
writeData(wb, "Stock_Data", combined_data)


# Sheet 3: Failed tickers
if (length(failed_tickers) > 0) {
  failed_df <- data.frame(Failed_Tickers = failed_tickers)
  addWorksheet(wb, "Failed_Tickers")
  writeData(wb, "Failed_Tickers", failed_df)
}

# Save workbook
saveWorkbook(wb, paste0("NY", success_count, "_stock_data.xlsx"), overwrite = TRUE)
write.csv(combined_data, paste0("NY", success_count, "_stock_data.csv"), row.names = FALSE)