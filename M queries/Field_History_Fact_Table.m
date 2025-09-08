// ============================================================================
// FIELD HISTORY FACT TABLE  (Created → "New"; durations incl. business days)
//  - Uses:  #"Opportunity Field History", #"Opportunity", #"Stage_Alias", #"Holidays"
// ============================================================================
let
    // -------------------- PARAMETERS --------------------
    WorkingDayStart = #time(9,0,0),
    WorkingDayEnd   = #time(17,0,0),

    // -------------------- SOURCES -----------------------
    HistBase    = #"Opportunity Field History",
    Opp         = #"Opportunity",
    StageAlias  = #"Stage_Alias",   // CanonicalStage, StageOrder
    HolidaysTbl = #"Holidays",      // must include [ObservedDate]::date

    // =========================================================================
    // HOLIDAYS LIST (buffer for performance)
    // =========================================================================
    HolidayList = List.Buffer( List.Transform(HolidaysTbl[ObservedDate], each Date.From(_)) ),

    // =========================================================================
    // CLOSEDATE LOOKUP  (latest valid CloseDate per Opportunity from history)
    // =========================================================================
    CloseHistOnly   = Table.SelectRows(HistBase, each Text.Lower([Field]) = "closedate"),
    CloseToDates    = Table.TransformColumns(
                         CloseHistOnly,
                         {{"NewValue", each try Date.From(_) otherwise null, type nullable date}}
                      ),
    CloseValid      = Table.SelectRows(CloseToDates, each [NewValue] <> null and [NewValue] <> #date(2099,12,31)),
    CloseSorted     = Table.Sort(CloseValid, {{"CreatedDate", Order.Ascending}}),
    LastClosePerOpp = Table.Group(
                         CloseSorted, {"OpportunityId"},
                         {
                           {"CloseDate_Latest", each List.Last([NewValue]), type date},
                           {"CloseDate_Set_TS", each List.Last([CreatedDate]), type datetime}
                         }
                      ),

    // =========================================================================
    // RAW HISTORY
    // =========================================================================
    HistKeep =
        Table.SelectColumns(
            HistBase,
            {"OpportunityId","Field","OldValue","NewValue","CreatedDate","CreatedById"}
        ),

    // =========================================================================
    // SYNTHETIC "ACTUAL GO LIVE" EVENTS
    // =========================================================================
    GoLiveHistOnly =
        Table.SelectRows(HistBase, each Text.Lower([Field]) = "actual_deployment_go_live_date__c"),

    GoLiveParsed =
        Table.TransformColumns(
            GoLiveHistOnly,
            {{"NewValue", each try Date.From(_) otherwise null, type nullable date}}
        ),

    GoLiveValid =
        Table.SelectRows(GoLiveParsed, each [NewValue] <> null),

    GoLiveLatestPerOpp =
        Table.Group(
            GoLiveValid, {"OpportunityId"},
            { {"GoLiveDate", each List.Last([NewValue]), type date} }
        ),

    GoLiveEventsBase =
        Table.TransformColumns(
            GoLiveLatestPerOpp,
            {{"GoLiveDate", each #datetime(Date.Year(_), Date.Month(_), Date.Day(_), 9, 0, 0), type datetime}}
        ),

    GoLiveEvents =
        Table.FromColumns(
            {
                GoLiveEventsBase[OpportunityId],
                List.Repeat({"StageName"}, Table.RowCount(GoLiveEventsBase)),      // Field
                List.Repeat({null},         Table.RowCount(GoLiveEventsBase)),     // OldValue
                List.Repeat({"Actual Go Live"}, Table.RowCount(GoLiveEventsBase)), // NewValue
                GoLiveEventsBase[GoLiveDate],                                      // CreatedDate
                List.Repeat({null},         Table.RowCount(GoLiveEventsBase))      // CreatedById
            },
            type table [
                OpportunityId = text,
                Field         = text,
                OldValue      = nullable text,
                NewValue      = text,
                CreatedDate   = datetime,
                CreatedById   = nullable text
            ]
        ),

    // =========================================================================
    // SYNTHETIC "INTAKE" EVENTS  (from Opportunity[Intake_Form_Received_Date__c])
    // =========================================================================
    IntakeKeep =
        Table.SelectColumns(
            Opp,
            {"Id","Intake_Form_Received_Date__c"}
        ),

    IntakeValid =
        Table.SelectRows(IntakeKeep, each [Intake_Form_Received_Date__c] <> null),

    IntakeEventsBase =
        Table.TransformColumns(
            IntakeValid,
            {{"Intake_Form_Received_Date__c",
               each #datetime(Date.Year(_), Date.Month(_), Date.Day(_), Time.Hour(WorkingDayStart), Time.Minute(WorkingDayStart), 0),
               type datetime}}
        ),

    IntakeEvents =
        Table.FromColumns(
            {
                IntakeEventsBase[Id],                                              // OpportunityId (will rename)
                List.Repeat({"Intake_Synthetic"}, Table.RowCount(IntakeEventsBase)), // Field tag
                List.Repeat({null},             Table.RowCount(IntakeEventsBase)), // OldValue
                List.Repeat({"Intake"},         Table.RowCount(IntakeEventsBase)), // NewValue (stage label)
                IntakeEventsBase[Intake_Form_Received_Date__c],                   // CreatedDate (StageTS anchor)
                List.Repeat({null},             Table.RowCount(IntakeEventsBase))  // CreatedById
            },
            type table [
                OpportunityId = text,
                Field         = text,
                OldValue      = nullable text,
                NewValue      = text,
                CreatedDate   = datetime,
                CreatedById   = nullable text
            ]
        ),

    // =========================================================================
    // COMBINE + FILTER EVENTS (Created + StageName, exclude "New", include Intake_Synthetic)
    // =========================================================================
    HistAugmented =
        Table.Combine({ HistKeep, GoLiveEvents, IntakeEvents }),

    HistFilteredForFact =
        Table.SelectRows(
            HistAugmented,
            each let f = Text.Lower([Field]) in
                (f = "created")
              or (f = "intake_synthetic")
              or (f = "stagename" and Text.Upper(Text.From([NewValue])) <> "NEW")
        ),

    // =========================================================================
    // ORDER EVENTS + StageEnd
    // =========================================================================
    Add_StageTS    = Table.AddColumn(HistFilteredForFact, "StageTS", each [CreatedDate], type datetime),
    Add_ToStageRaw = Table.AddColumn(Add_StageTS, "ToStageRaw",
                        each if Text.Lower([Field]) = "created" then "New"
                             else if Text.Lower([Field]) = "intake_synthetic" then "Intake"
                             else [NewValue],
                        type text),
    Add_FromStageRaw = Table.AddColumn(Add_ToStageRaw, "FromStageRaw",
                        each if Text.Lower([Field]) = "created" or Text.Lower([Field]) = "intake_synthetic"
                             then null else [OldValue],
                        type text),

    SortEvents = Table.Sort(Add_FromStageRaw, {{"OpportunityId", Order.Ascending}, {"StageTS", Order.Ascending}}),
    GroupByOpp =
        Table.Group(
            SortEvents, {"OpportunityId"},
            {
              { "Rows",
                (t) =>
                  let
                    t1 = Table.AddIndexColumn(t, "ix", 0, 1, Int64.Type),
                    nextTS = List.Skip(t1[StageTS], 1) & {null},
                    t2 = Table.AddColumn(t1, "StageEnd", each nextTS{[ix]}, type nullable datetime)
                  in t2,
                type table
              }
            }
        ),
    ExpandRows =
        Table.ExpandTableColumn(
            GroupByOpp, "Rows",
            {"Field","OldValue","NewValue","CreatedDate","CreatedById","StageTS","ToStageRaw","FromStageRaw","ix","StageEnd"},
            {"Field","OldValue","NewValue","CreatedDate","CreatedById","StageStart","ToStageRaw","FromStageRaw","ix","StageEnd"}
        ),

    // =========================================================================
    // Explicit type conversion
    // =========================================================================
    ChangeTypes =
        Table.TransformColumnTypes(
            ExpandRows,
            {
                {"StageStart", type datetime},
                {"StageEnd",   type datetime}
            }
        ),

    // =========================================================================
    // CANONICALIZE KEYS (trim+lowercase) for join, preserve original labels
    // =========================================================================
    Add_StageKeys =
        Table.AddColumn(
            Table.AddColumn(ChangeTypes, "FromStageKey", each if [FromStageRaw]=null then null else Text.Lower(Text.Trim([FromStageRaw]))),
            "ToStageKey",   each if [ToStageRaw]=null then null else Text.Lower(Text.Trim([ToStageRaw]))
        ),

    RenameCanon =
        Table.RenameColumns(Add_StageKeys, {{"FromStageRaw","FromStage"},{"ToStageRaw","ToStage"}}),

    // Canonicalize Stage_Alias the same way
    StageAliasKeyed =
        Table.AddColumn(
            Table.TransformColumns(StageAlias, {{"CanonicalStage", each Text.Trim(_), type text}}),
            "StageKey", each Text.Lower([CanonicalStage])
        ),

    // =========================================================================
    // JOIN STAGE ALIAS
    // =========================================================================
    JoinAliasTo   = Table.NestedJoin(RenameCanon, {"ToStageKey"}, StageAliasKeyed, {"StageKey"}, "AliasTo", JoinKind.LeftOuter),
    ExpandAliasTo = Table.ExpandTableColumn(JoinAliasTo, "AliasTo", {"StageOrder"}, {"StageOrder"}),

    // =========================================================================
    // MERGE LATEST CLOSEDATE
    // =========================================================================
    MergeClose  = Table.NestedJoin(ExpandAliasTo, {"OpportunityId"}, LastClosePerOpp, {"OpportunityId"}, "CloseLkp", JoinKind.LeftOuter),
    ExpandClose = Table.ExpandTableColumn(MergeClose, "CloseLkp", {"CloseDate_Latest","CloseDate_Set_TS"}, {"CloseDate_Latest","CloseDate_Set_TS"}),

    // =========================================================================
    // TERMINAL STAGE HANDLING (Complete/Removed)
    // =========================================================================
    StageEnd_Final =
        Table.AddColumn(
            ExpandClose, "StageEnd_Final",
            each
                if [StageEnd] <> null then [StageEnd]
                else if [ToStage] = "Complete" and [CloseDate_Latest] <> null
                    then #datetime(Date.Year([CloseDate_Latest]), Date.Month([CloseDate_Latest]), Date.Day([CloseDate_Latest]), 17, 0, 0)
                else if List.Contains({"Complete","Removed"}, [ToStage])
                    then [StageStart]
                else null,
            type nullable datetime
        ),

    DropOldStageEnd = try Table.RemoveColumns(StageEnd_Final, {"StageEnd"}) otherwise StageEnd_Final,
    SwapEnd         = Table.RenameColumns(DropOldStageEnd, {{"StageEnd_Final","StageEnd"}}),

    // =========================================================================
    // DURATIONS
    // =========================================================================
    Add_DurationDays =
        Table.AddColumn(
            SwapEnd, "DurationDays",
            each if [StageEnd] = null then null else Duration.TotalDays([StageEnd] - [StageStart]),
            type nullable number
        ),

    Add_BusinessDays =
        Table.AddColumn(
            Add_DurationDays, "BusinessDays",
            each
              if [StageEnd] = null then null
              else
                let
                  s = Date.From([StageStart]),
                  e = Date.AddDays(Date.From([StageEnd]), -1),   // exclusive end
                  span = if e < s then 0 else Duration.Days(e - s) + 1,
                  dates = if span <= 0 then {} else List.Dates(s, span, #duration(1,0,0,0)),
                  weekdays = List.Select(dates, each Date.DayOfWeek(_, Day.Monday) < 5),
                  noHols = List.Difference(weekdays, HolidayList),
                  cnt = List.Count(noHols)
                in cnt,
            Int64.Type
        ),

    Add_BusinessHours =
        Table.AddColumn(
            Add_BusinessDays, "BusinessHours",
            each
              if [StageEnd] = null then null
              else
                let
                  startDT = [StageStart],
                  endDT   = [StageEnd],
                  startDate = Date.From(startDT),
                  endDate   = Date.From(endDT),
                  nDays     = Duration.Days(endDate - startDate) + 1,
                  allDays   = if nDays <= 0 then {} else List.Dates(startDate, nDays, #duration(1,0,0,0)),
                  weekdays  = List.Select(allDays, each Date.DayOfWeek(_, Day.Monday) < 5),
                  workdays  = List.Difference(weekdays, HolidayList),
                  hoursPerDay = List.Transform(
                    workdays,
                    (d as date) =>
                      let
                        ws = #datetime(Date.Year(d), Date.Month(d), Date.Day(d),
                                       Time.Hour(WorkingDayStart), Time.Minute(WorkingDayStart), 0),
                        we = #datetime(Date.Year(d), Date.Month(d), Date.Day(d),
                                       Time.Hour(WorkingDayEnd),   Time.Minute(WorkingDayEnd),   0),
                        dayStart = if Date.From(startDT) = d then if startDT > ws then startDT else ws else ws,
                        dayEnd   = if Date.From(endDT)   = d then if endDT   < we then endDT   else we else we,
                        hrs = List.Max({ 0, Duration.TotalHours(dayEnd - dayStart) })
                      in hrs
                  ),
                  total = List.Sum(hoursPerDay)
                in total,
            type number
        ),

    Add_IsOpenStage = Table.AddColumn(Add_BusinessHours, "IsOpenStage", each [StageEnd] = null, type logical),

    // =========================================================================
    // JOIN OPPORTUNITY FIELDS
    // =========================================================================
    OppKeep =
        let cols = List.Intersect({
                {"Id","Name","AccountId","OwnerId","StageName","IsRunMaintain"},
                 Table.ColumnNames(Opp)
        })
        in Table.SelectColumns(Opp, cols),
    JoinOpp =
        Table.NestedJoin(Add_IsOpenStage, {"OpportunityId"}, OppKeep, {"Id"}, "Opp", JoinKind.LeftOuter),
    ExpandOpp =
        Table.ExpandTableColumn(
            JoinOpp, "Opp",
            {"Name","AccountId","OwnerId","StageName","IsRunMaintain"},
            {"Opp_Name","Opp_AccountId","Opp_OwnerId","Opp_StageName","IsRunMaintain"}
        ),

    // =========================================================================
    // FINAL
    // =========================================================================
    Final =
        Table.SelectColumns(
            ExpandOpp,
            {
              "OpportunityId","Opp_Name","Opp_AccountId","Opp_OwnerId","IsRunMaintain",
              "FromStage","ToStage","StageOrder",
              "StageStart","StageEnd",
              "DurationDays","BusinessDays","BusinessHours",
              "Field","OldValue","NewValue","CreatedById","IsOpenStage",
              "CloseDate_Latest","CloseDate_Set_TS"   // QA / traceability
            }
        ),

    // (Optional) spot-check Intake
    // #"Filtered Rows" = Table.SelectRows(Final, each [ToStage] = "Intake")
    Output = Final
in
    Output