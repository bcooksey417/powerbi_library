// ============================================================================
// OPPORTUNITY FIELD HISTORY  (scoped to filtered Opps; keep Created + StageName)
// ============================================================================
let
    // ------------------------------------------------------------------------
    // SOURCE  (point at your existing initial step for OpportunityFieldHistory)
    // ------------------------------------------------------------------------
    HistRaw = #"OppFieldHistory_source",
    // ------------------------------------------------------------------------
    // LIMIT TO EDI OPPORTUNITY 
    // ------------------------------------------------------------------------
    OppFiltered = #"Opportunity",  // this is already filtered in its own query
   
    OppIDs      = Table.Distinct(
                Table.RenameColumns(
                    Table.SelectColumns(OppFiltered, {"Id"}),
                    {{"Id","OppId"}}   // rename to avoid clash
                )
            ),


    // =========================================================================
    // INNER JOIN: Keep only history rows whose OpportunityId is in OppIDs
    // =========================================================================
    HistFiltered =
        Table.Join(
            HistRaw,   "OpportunityId",
            OppIDs,    "OppId",
            JoinKind.Inner
        ),

    // ------------------------------------------------------------------------
    // KEEP NEEDED COLUMNS & TYPES (do not strip others earlier than this)
    // ------------------------------------------------------------------------
    HistKeep =
        Table.SelectColumns(
            HistFiltered,
            {"OpportunityId","Field","OldValue","NewValue","CreatedDate","CreatedById"}
        ),

    // =========================================================================
    // TYPE CONVERSIONS
    // =========================================================================    

    HistTyped =
        Table.TransformColumnTypes(
            HistKeep,
            {
              {"OpportunityId", type text},
              {"Field",         type text},
              {"OldValue",      type text},
              {"NewValue",      type text},
              {"CreatedDate",   type datetime},
              {"CreatedById",   type text}
            },
            "en-US"
        ),

    // ------------------------------------------------------------------------
    // EVENTS WE CARE ABOUT for the pipeline model:
    //  - Keep "Created" (system creation)
    //  - Keep all "StageName" events (we will exclude StageName='New' in the FACT to avoid duplicate "New")
    // ------------------------------------------------------------------------
    HistEventsOnly =
        Table.SelectRows(
            HistTyped,
            each let f = Text.Lower([Field]) in (f = "created") or (f = "stagename") or (f = "closedate") or (f = "actual_deployment_go_live_date__c")
        )

in
    HistEventsOnly