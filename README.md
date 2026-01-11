## Traffic Stop Analysis - Driver Risk & Enforcement Patterns

This project analyzes over 2 million traffic stop records from a single county in Maryland to identify patterns in enforcement behavior, driver risk, and temporal trends. 
Using Python, SQL, and Power BI, the analysis focuses on when and why traffic stops are most likely to occur, translating raw public data into actionable insights.

### Source
- Source: Public traffic stop records
  https://data.montgomerycountymd.gov/Public-Safety/Traffic-Violations/4mse-ku6q/about_data
- Timeframe: 2012–2025
- Records: ~2.0 million
- Scope: Montgomery county (Maryland)

### Key Preparations
- Normalizing vehicle make values
- Parsing description to map reason for stop
- Parsing and standardizing date/time fields
- Handling null and unknown categories
- Creating derived time-based features (hour of the day)

### Tools & Technologies
- Python (pandas & numpy)
- SQL (PostgreSQL)
- Power BI
