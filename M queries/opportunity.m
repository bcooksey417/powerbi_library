// ============================================================================
// OPPORTUNITY  (keep ALL columns; add helper flags only)
// ============================================================================
let
    // ------------------------------------------------------------------------
    // SOURCE  (point at your existing initial step for Opportunity)
    // ------------------------------------------------------------------------
    Opportunity_source = #"Opportunity_source",
    // ------------------------------------------------------------------------
    // FILTER: EDI Carrier Integration scope (preserve all columns)
    // ------------------------------------------------------------------------
    #"Filtered EDI" =
        Table.SelectRows(
            Opportunity_source,
            each [Opportunity_Record_Type_Formula__c] = "EDI Carrier Integration"
        ),

    // ------------------------------------------------------------------------
    // TYPES for a few key fields (we do NOT drop any columns)
    // ------------------------------------------------------------------------
    #"Typed (key fields only)" =
        Table.TransformColumnTypes(
            #"Filtered EDI",
            {
              {"CreatedDate", type datetime},
              {"CloseDate", type date},
              {"Intake_Form_Received_Date__c", type date},
              {"Actual_Deployment_Go_Live_Date__c", type nullable date}
            },
            "en-US"
        ),

    // ------------------------------------------------------------------------
    // FLAGS (non-destructive; keep all original cols)
    //  - IsRunMaintain: Name contains any Run&Maintain variant
    //  - IsEDIOnboardingEligible: in-scope EDI AND not Run&Maintain
    // ------------------------------------------------------------------------
    Add_IsRunMaintain =
        Table.AddColumn(
            #"Typed (key fields only)", "IsRunMaintain",
            each let nm = Text.Upper(Text.From(Record.FieldOrDefault(_, "Name", ""))) in
                 Text.Contains(nm,"RUN&MAINTAIN")
              or Text.Contains(nm,"RUN & MAINTAIN")
              or Text.Contains(nm,"RUN/MAINTAIN")
              or Text.Contains(nm,"RUN AND MAINTAIN"),
            type logical
        ),

    // ------------------------------------------------------------------------
    // CloseDate normalization (treat 12/31/2099 as placeholder → null)
    // ------------------------------------------------------------------------
    Add_CloseDate_Normalized =
        Table.AddColumn(
            Add_IsRunMaintain, "CloseDate_Normalized",
            each if [CloseDate] = #date(2099,12,31) then null else [CloseDate],
            type nullable date
        ),
    #"Renamed Columns" = Table.RenameColumns(Add_CloseDate_Normalized,{{"System_Setup__c", "TMS"}})

in
    #"Renamed Columns"