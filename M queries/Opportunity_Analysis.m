// ============================================================================
// OPPORTUNITY_ANALYSIS  (analysis-only; Intake QA + CloseDate QA + Created→New lag)
//  - Uses:  #"Opportunity", #"Opportunity Field History"
//  - Outputs 1 row per Opportunity Id
// ============================================================================
let
    // ------------------------------------------------------------------------
    // SOURCES
    // ------------------------------------------------------------------------
    Opp  = #"Opportunity",
    Hist = #"Opportunity Field History",

    // ------------------------------------------------------------------------
    // FIRST TIME StageName flipped to "NEW"  (for Created → New lag)
    // ------------------------------------------------------------------------
    KeepStage     = Table.SelectRows(Hist, each Text.Lower([Field]) = "stagename"),
    NewOnly       = Table.SelectRows(KeepStage, each Text.Upper(Text.From([NewValue])) = "NEW"),
    NewSorted     = Table.Sort(NewOnly, {{"CreatedDate", Order.Ascending}}),
    FirstNewByOpp = Table.Group(
                        NewSorted,
                        {"OpportunityId"},
                        { {"FirstNew_TS", each List.First([CreatedDate]), type datetime} }
                    ),

    // ------------------------------------------------------------------------
    // LATEST VALID CloseDate from history (ignore 12/31/2099 placeholders)
    // ------------------------------------------------------------------------
    CloseHistOnly = Table.SelectRows(Hist, each Text.Lower([Field]) = "closedate"),
    CloseToDates  = Table.TransformColumns(
                        CloseHistOnly,
                        {{"NewValue", each try Date.From(_) otherwise null, type nullable date}}
                    ),
    CloseValid    = Table.SelectRows(CloseToDates, each [NewValue] <> null and [NewValue] <> #date(2099,12,31)),
    CloseSorted   = Table.Sort(CloseValid, {{"CreatedDate", Order.Ascending}}),
    LastClosePerOpp =
        Table.Group(
            CloseSorted,
            {"OpportunityId"},
            {
                {"CloseDate_Latest", each List.Last([NewValue]), type date},
                {"CloseDate_Set_TS", each List.Last([CreatedDate]), type datetime}
            }
        ),

    // ------------------------------------------------------------------------
    // BUILD ANALYSIS ROW (one per Opportunity)
    // ------------------------------------------------------------------------
    OppKeep =
        Table.SelectColumns(
            Opp,
            {"Id","CreatedDate","Intake_Form_Received_Date__c","CloseDate_Normalized"}
        ),

    // Merge FirstNew timestamp
    MergeFirstNew  = Table.NestedJoin(OppKeep, {"Id"}, FirstNewByOpp, {"OpportunityId"}, "FirstNew", JoinKind.LeftOuter),
    ExpandFirstNew = Table.ExpandTableColumn(MergeFirstNew, "FirstNew", {"FirstNew_TS"}, {"FirstNew_TS"}),

    // Created → New lag (hours) + flag
    AddLagHours =
        Table.AddColumn(
            ExpandFirstNew, "Created_New_Lag_Hours",
            each if [FirstNew_TS] = null or [CreatedDate] = null
                 then null
                 else Duration.TotalHours([FirstNew_TS] - [CreatedDate]),
            type number
        ),
    AddLagFlag =
        Table.AddColumn(
            AddLagHours, "Created_New_Lag_GT24h",
            each [Created_New_Lag_Hours] <> null and Number.Abs([Created_New_Lag_Hours]) > 24,
            type logical
        ),

    // Intake QA
    AddIntakeMissing =
        Table.AddColumn(
            AddLagFlag, "IntakeDate_Missing",
            each [Intake_Form_Received_Date__c] = null, type logical
        ),
    AddIntakeAfterCreate =
        Table.AddColumn(
            AddIntakeMissing, "Intake_After_Create_Flag",
            each try Date.From([Intake_Form_Received_Date__c]) > Date.From([CreatedDate]) otherwise false,
            type logical
        ),

    // Merge latest valid CloseDate from history
    MergeClose     = Table.NestedJoin(AddIntakeAfterCreate, {"Id"}, LastClosePerOpp, {"OpportunityId"}, "CloseHist", JoinKind.LeftOuter),
    ExpandClose    = Table.ExpandTableColumn(MergeClose, "CloseHist",
                        {"CloseDate_Latest","CloseDate_Set_TS"},
                        {"CloseDate_Latest","CloseDate_Set_TS"}),

    // Mismatch flag (current Opp CloseDate vs latest history CloseDate)
    AddCloseMismatch =
        Table.AddColumn(
            ExpandClose, "CloseDate_Mismatch_Flag",
            each try (
                    [CloseDate_Normalized] <> null
                and [CloseDate_Latest]    <> null
                and [CloseDate_Normalized] <> [CloseDate_Latest]
            ) otherwise false,
            type logical
        ),

    // ------------------------------------------------------------------------
    // FINAL (1 row per Opportunity)
    // ------------------------------------------------------------------------
    Final =
        Table.SelectColumns(
            AddCloseMismatch,
            {
              "Id",
              "CreatedDate",
              "FirstNew_TS",
              "Created_New_Lag_Hours","Created_New_Lag_GT24h",
              "Intake_Form_Received_Date__c","IntakeDate_Missing","Intake_After_Create_Flag",
              "CloseDate_Normalized",
              "CloseDate_Latest","CloseDate_Set_TS","CloseDate_Mismatch_Flag"
            }
        )
in
    Final