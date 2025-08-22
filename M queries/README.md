# M Queries

## create date table DateTable

UPDATE: Create the holidays table query first. I've updated the date table to reference the us federal holidays query
Joins your DateTable to the Holidays query on Date = ObservedDate.
Adds a column HolidayName with the holiday name (or null if not a holiday).
Adds a boolean IsHoliday flag.

📝 Notes

This creates a continuous calendar from 2019 → next year (adjust StartYear as needed).

Includes:

Year, Quarter, Month (number, name, short), Day, Week, Year-Month

Fiscal Year & Fiscal Quarter (assumes July start, change FiscalStartMonth if needed).

Name the query DateTable.

In the model, mark it as Date Table (Model view → Table tools → Mark as Date Table → choose Date column).

NOTE: create the holdays table query, the updates date table query ref

<img width="597" height="362" alt="image" src="https://github.com/user-attachments/assets/d576b409-fc3b-4f08-8a64-42e11cd608ed" />

## create use federal holidays table - Holidays
Holidays builds a US‑federal holiday table for 2024–2026, including proper “observed” dates when a holiday lands on a weekend.
