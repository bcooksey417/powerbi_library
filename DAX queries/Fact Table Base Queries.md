## counts

Total Integrations :=
DISTINCTCOUNT ( 'Field History Fact Table'[OpportunityId] )

Total Stage Records :=
COUNTROWS ( 'Field History Fact Table' )

Active Opportunities :=
CALCULATE (
    DISTINCTCOUNT ( 'Field History Fact Table'[OpportunityId] ),
    FILTER ( 'Field History Fact Table', 'Field History Fact Table'[IsOpenStage] = TRUE )
)

## durations
Avg Duration (Days) :=
AVERAGE ( 'Field History Fact Table'[DurationDays] )

Avg Business Days :=
AVERAGE ( 'Field History Fact Table'[BusinessDays] )

Avg Business Hours :=
AVERAGE ( 'Field History Fact Table'[BusinessHours] )

Total Duration (Days) :=
SUM ( 'Field History Fact Table'[DurationDays] )

## blocked duration
Blocked Duration (Business Days) :=
CALCULATE (
    SUM ( 'Field History Fact Table'[BusinessDays] ),
    FILTER ( 'Field History Fact Table', 'Field History Fact Table'[ToStage] = "Blocked" )
)

Avg Blocked Duration :=
CALCULATE (
    AVERAGE ( 'Field History Fact Table'[BusinessDays] ),
    FILTER ( 'Field History Fact Table', 'Field History Fact Table'[ToStage] = "Blocked" )
)

## lifecycle KPIs
First StageStart Date :=
CALCULATE ( MIN ( 'Field History Fact Table'[StageStart] ) )

Last StageEnd Date :=
CALCULATE ( MAX ( 'Field History Fact Table'[StageEnd] ) )

Intake to Complete (Business Days) :=
VAR IntakeDate =
    CALCULATE (
        MIN ( 'Field History Fact Table'[StageStart] ),
        FILTER ( 'Field History Fact Table', 'Field History Fact Table'[ToStage] = "Intake" )
    )
VAR CompleteDate =
    CALCULATE (
        MIN ( 'Field History Fact Table'[StageStart] ),
        FILTER ( 'Field History Fact Table', 'Field History Fact Table'[ToStage] = "Complete" )
    )
RETURN
IF (
    NOT ISBLANK ( IntakeDate ) && NOT ISBLANK ( CompleteDate ),
    CALCULATE (
        COUNTROWS (
            FILTER (
                'DateTable',
                'DateTable'[Date] >= IntakeDate
                  && 'DateTable'[Date] <= CompleteDate
                  && 'DateTable'[IsBusinessDay] = TRUE
            )
        )
    )
)
## date intelligence
Last Refresh Date :=
MAX ( 'DateTable'[Date] )

Integrations YTD :=
CALCULATE (
    [Total Integrations],
    DATESYTD ( 'DateTable'[Date] )
)

Integrations MTD :=
CALCULATE (
    [Total Integrations],
    DATESMTD ( 'DateTable'[Date] )
)

Integrations Last Year :=
CALCULATE (
    [Total Integrations],
    SAMEPERIODLASTYEAR ( 'DateTable'[Date] )
)
