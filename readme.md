# Average Working Hours by Marital Status (Hadoop MapReduce)

This project calculates the **average working hours per week** for individuals grouped by their **marital status**, using the Hadoop MapReduce framework.

## **Project Structure**
- `src/main/java/org/example`
    - `MaritalStatusMapper.java`: Extracts marital status and working hours from the dataset.
    - `AverageReducer.java`: Aggregates data and calculates the average working hours per marital status.
    - `AverageHoursByMaritalStatus.java`: Driver program to configure and run the MapReduce job.

## **Dataset**
The input dataset is expected to be in CSV format with the following columns:
- Marital status (e.g., "Married", "Single")
- Hours worked per week (numeric field)

**Example Input Data:**
```csv
id,age,gender,marital_status,occupation,hours_per_week,...
1,25,Male,Single,Engineer,40,...
2,30,Female,Married,Doctor,50,...
