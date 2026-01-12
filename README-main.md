# Traffic Stop Analysis - Driver Risk & Enforcement Patterns

This project analyzes over 2 million traffic stop records from a single county in Maryland to identify patterns in enforcement behavior, driver risk, and temporal trends. 
Using Python, SQL, and Power BI, the analysis focuses on when and why traffic stops are most likely to occur, translating raw public data into actionable insights.

## Source
- Source: Public traffic stop records
  https://data.montgomerycountymd.gov/Public-Safety/Traffic-Violations/4mse-ku6q/about_data
- Timeframe: 2012–2025
- Records: ~2.0 million
- Scope: Montgomery county (Maryland)

## Key Preparations
- Normalizing vehicle make values
- Parsing description to map reason for stop
- Parsing and standardizing date/time fields
- Handling null and unknown categories
- Creating derived time based features (hour of the day)

## Tools & Technologies
- Python (pandas & numpy)
- SQL (PostgreSQL)
- Power BI

## Key Questions
- When are drivers most likely to be stopped?
- Which driver and vehicle characteristics are associated with higher stop frequency?
- Which violations most commonly lead to traffic stops?
- How do enforcement patterns differ during low traffic hours?
- Are late night stops disproportionately frequent relative to traffic volume?

## Objective
The goal of this analysis is to identify when and why traffic stops are most likely to occur, and to translate enforcement data into insights that can support driver awareness, risk assessment, and policy evaluation.

## Key Insights
- Traffic stops peak disproportionately during late night hours (10–11 PM), despite lower overall traffic volume.
- Speeding is the most common reason for traffic stops across all time periods.
- Lower traffic density at night increases the likelihood of individual vehicles being singled out for enforcement.
- Stop frequency varies across observable driver and vehicle attributes, with higher counts among commonly represented vehicle categories, reflecting exposure rather than elevated risk.

## Dashboard & Visualizations
An interactive Power BI dashboard was created to visualize:
- Hourly distribution of traffic stops
- Violation type breakdowns
- Geographic concentration of stops

The dashboard is designed to support quick exploration of enforcement patterns and time-based risk.

![Main Page](images/dashboard_page_1.png)
![Map Page](images/dashboard_page_2.png)

## Limitations & Assumptions
- The dataset is limited to a single county in Maryland and may not generalize to other regions.
- Around 50% of the stop reasons and 14% of car makes could not be accurately mapped.
- Traffic volume data was not directly available; conclusions are based on stop frequency patterns.
- Enforcement practices may vary over time due to policy or staffing changes.
