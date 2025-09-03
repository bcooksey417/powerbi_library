let
  // =========================================================================
  // PARAMETERS
  // =========================================================================
  OpenStageEndBehavior = "Null",   // "Now" | "Null"
  WorkingDayStart      = #time(9,0,0),   // Business day start (9:00)
  WorkingDayEnd        = #time(17,0,0),  // Business day end   (17:00)

  // =========================================================================
  // SOURCES (reference EXISTING queries)
  // =========================================================================
  SourceHistory = #"Opportunity Field History",
  SourceOpp     = #"Opportunity",
  SourceAcct    = #"Account",
  SourceUser    = #"User",
  SourceHol     = #"Holidays",   // holiday table (must have ObservedDate column)

  // =========================================================================
  // HOLIDAYS LIST (buffer for performance)
  // =========================================================================
  HolidaysList = List.Buffer( List.Transform(SourceHol[ObservedDate], each Date.From(_)) ),

  // =========================================================================
  // KEEP NEEDED COLUMNS & FILTER EVENTS
  // =========================================================================
  HistKeep =
      Table.SelectColumns(
        SourceHistory,
        {"OpportunityId","Field","OldValue","NewValue","CreatedDate","CreatedById"}
      ),
  HistFiltered =
      Table.SelectRows(
        HistKeep,
        each Text.Lower([Field]) = "stagename" or Text.Lower([Field]) = "created"
      ),
  HistTyped =
      Table.TransformColumnTypes(
        HistFiltered,
        {
          {"OpportunityId", type text},
          {"Field",         type text},
          {"OldValue",      type text},
          {"NewValue",      type text},
          {"CreatedDate",   type datetime},
          {"CreatedById",   type text}
        }
      ),

  // =========================================================================
  // NORMALIZE "CREATED" EVENT → pseudo-stage row (ToStage = Request Confirmed)
  // =========================================================================
  AddToStageForCreated =
      Table.AddColumn(
        HistTyped, "ToStageRaw",
        each if Text.Lower([Field]) = "created" then "Request Confirmed" else [NewValue],
        type text
      ),
  AddFromStageForCreated =
      Table.AddColumn(
        AddToStageForCreated, "FromStageRaw",
        each if Text.Lower([Field]) = "created" then null else [OldValue],
        type text
      ),
  HistMeaningful =
      Table.SelectRows(AddFromStageForCreated, each [ToStageRaw] <> null and Text.Trim([ToStageRaw]) <> ""),
  HistSorted =
      Table.Sort(HistMeaningful, {{"OpportunityId", Order.Ascending}, {"CreatedDate", Order.Ascending}}),

  // =========================================================================
  // BUILD DURATIONS BY OPPORTUNITY (LEAD CreatedDate as StageEnd)
  // =========================================================================
  Grouped =
      Table.Group(
        HistSorted, {"OpportunityId"},
        {
          {
            "Rows",
            (t) =>
              let
                t1 = Table.AddIndexColumn(t, "ix", 0, 1, Int64.Type),
                nextDateList  = List.Skip(t1[CreatedDate], 1) & {null},
                nextStageList = List.Skip(t1[ToStageRaw],   1) & {null},
                t2 = Table.AddColumn(t1, "StageEndRaw", each nextDateList{[ix]}, type nullable datetime),
                t3 = Table.AddColumn(t2, "NextStageRaw", each nextStageList{[ix]}, type nullable text),

                // Choose open-stage handling
                t4 =
                  if OpenStageEndBehavior = "Now" then
                    Table.TransformColumns(
                      t3,
                      {{"StageEndRaw", (d) => if d = null then DateTime.LocalNow() else d, type datetime}}
                    )
                  else t3,

                // Calendar days
                t5 = Table.AddColumn(
                       t4, "DurationDaysRaw",
                       each if [StageEndRaw] = null then null else Duration.Days([StageEndRaw] - [CreatedDate]),
                       Int64.Type
                     ),

                // =========================================================================
                // BUSINESS DAYS: Mon–Fri, excluding holidays
                // =========================================================================
                t6 = Table.AddColumn(
                       t5, "BusinessDaysRaw",
                       each if [StageEndRaw] = null then null else
                         let
                           s = Date.From([CreatedDate]),
                           e = Date.AddDays(Date.From([StageEndRaw]), -1),    // exclusive end
                           span = if e < s then 0 else Duration.Days(e - s) + 1,
                           dates = if span <= 0 then {} else List.Dates(s, span, #duration(1,0,0,0)),
                           weekdays = List.Select(dates, each Date.DayOfWeek(_, Day.Monday) < 5),
                           noHols = List.Difference(weekdays, HolidaysList),
                           cnt = List.Count(noHols)
                         in cnt,
                       Int64.Type
                     ),

                // =========================================================================
                // BUSINESS HOURS: Mon–Fri, 9–5, excluding holidays
                // =========================================================================
                t7 = Table.AddColumn(
                       t6, "BusinessHoursRaw",
                       each if [StageEndRaw] = null then null else
                         let
                           sDate = Date.From([CreatedDate]),
                           eDate = Date.AddDays(Date.From([StageEndRaw]), -1),
                           nDays = Duration.Days(eDate - sDate) + 1,
                           allDays = if nDays <= 0 then {} else List.Dates(sDate, nDays, #duration(1,0,0,0)),
                           weekdays = List.Select(allDays, each Date.DayOfWeek(_, Day.Monday) < 5),
                           workdays = List.Difference(weekdays, HolidaysList),
                           hoursPerDay = List.Transform(
                             workdays,
                             (d as date) =>
                               let
                                 ws = #datetime(Date.Year(d), Date.Month(d), Date.Day(d),
                                                Time.Hour(WorkingDayStart), Time.Minute(WorkingDayStart), 0),
                                 we = #datetime(Date.Year(d), Date.Month(d), Date.Day(d),
                                                Time.Hour(WorkingDayEnd), Time.Minute(WorkingDayEnd), 0),
                                 ps = if Date.From([CreatedDate]) = d then if [CreatedDate] > ws then [CreatedDate] else ws else ws,
                                 pe = if Date.From([StageEndRaw]) = d then if [StageEndRaw] < we then [StageEndRaw] else we else we,
                                 hrs = List.Max({0, Duration.TotalHours(pe - ps)})
                               in hrs
                           ),
                           total = List.Sum(hoursPerDay)
                         in total,
                       type number
                     )
              in
                t7,
            type table
          }
        }
      ),
  Expanded =
      Table.ExpandTableColumn(
        Grouped, "Rows",
        {"FromStageRaw","ToStageRaw","CreatedDate","StageEndRaw","NextStageRaw",
         "DurationDaysRaw","BusinessDaysRaw","BusinessHoursRaw","CreatedById"},
        {"FromStage","ToStage","StageStart","StageEnd","NextStage",
         "DurationDays","BusinessDays","BusinessHours","Hist_CreatedById"}
      ),

  // =========================================================================
  // CANONICAL STAGE MAPPING (clean labels for KPIs)
  // =========================================================================
  CanonMap = #table(
    {"raw","canon"},
    {
      {"new","New"},
      {"intake","Intake"},
      {"intake form","Intake"},
      {"request submitted","Intake"},
      {"request confirmed","Request Confirmed"},
      {"created","Request Confirmed"},
      {"engagement","Engagement"},
      {"staging engagement","Engagement"},
      {"ready to assign","Ready to Assign"},
      {"development","Development"},
      {"dev","Development"},
      {"testing","Testing"},
      {"pending deployment","Pending Deployment"},
      {"post go live","Post Go Live"},
      {"post-go-live","Post Go Live"},
      {"complete","Complete"},
      {"closed","Closed"},
      {"blocked","Blocked"},
      {"removed","Removed"}
    }
  ),
  ToCanon = (s as nullable text) as nullable text =>
    let lower = if s=null then null else Text.Lower(Text.Trim(s)),
        match = try Table.SelectRows(CanonMap, each [raw] = lower){0}[canon] otherwise s
    in match,
  CanonApplied =
      Table.TransformColumns(
        Expanded,
        {
          {"FromStage", ToCanon, type text},
          {"ToStage",   ToCanon, type text},
          {"NextStage", ToCanon, type text}
        }
      ),

  // =========================================================================
  // DENORMALIZE DIMENSIONS (Opportunity / Account / User)
  // =========================================================================
  OppKeep =
      let cols = List.Intersect({
              {"Id","Name","AccountId","OwnerId","StageName","CloseDate"},
              Table.ColumnNames(SourceOpp)
            })
      in Table.TransformColumnTypes(Table.SelectColumns(SourceOpp, cols), {{"Id", type text},{"AccountId", type text},{"OwnerId", type text}}),
  MergeOpp = Table.NestedJoin(CanonApplied, {"OpportunityId"}, OppKeep, {"Id"}, "Opp", JoinKind.LeftOuter),
  ExpandOpp = Table.ExpandTableColumn(MergeOpp, "Opp",
               {"Name","AccountId","OwnerId","StageName","CloseDate"},
               {"Opp_Name","Opp_AccountId","Opp_OwnerId","Opp_StageName","Opp_CloseDate"}),

  AcctKeep = Table.TransformColumnTypes(Table.SelectColumns(SourceAcct, {"Id","Name"}), {{"Id", type text}}),
  MergeAcct = Table.NestedJoin(ExpandOpp, {"Opp_AccountId"}, AcctKeep, {"Id"}, "Acct", JoinKind.LeftOuter),
  ExpandAcct = Table.ExpandTableColumn(MergeAcct, "Acct", {"Name"}, {"Account_Name"}),

  UserKeep = Table.TransformColumnTypes(Table.SelectColumns(SourceUser, {"Id","Name"}), {{"Id", type text}}),
  MergeOwner = Table.NestedJoin(ExpandAcct, {"Opp_OwnerId"}, UserKeep, {"Id"}, "Owner", JoinKind.LeftOuter),
  ExpandOwner = Table.ExpandTableColumn(MergeOwner, "Owner", {"Name"}, {"Owner_Name"}),

  MergeHistUser = Table.NestedJoin(ExpandOwner, {"Hist_CreatedById"}, UserKeep, {"Id"}, "HistUser", JoinKind.LeftOuter),
  ExpandHistUser = Table.ExpandTableColumn(MergeHistUser, "HistUser", {"Name"}, {"HistUser_Name"}),

  // =========================================================================
  // FINAL SHAPE & TYPES
  // =========================================================================
  WithIsOpenStage =
      Table.AddColumn(
        ExpandHistUser,
        "IsOpenStage",
        each [StageEnd] = null,
        type logical
      ),

  AddStageOrder =
      Table.AddColumn(
        WithIsOpenStage,
        "StageOrder",
        each
          if [ToStage] = "Intake" then 1
          else if [ToStage] = "Request Confirmed" then 2
          else if [ToStage] = "Engagement" then 3
          else if [ToStage] = "Ready to Assign" then 4
          else if [ToStage] = "Development" then 5
          else if [ToStage] = "Testing" then 6
          else if [ToStage] = "Pending Deployment" then 7
          else if [ToStage] = "Post Go Live" then 8
          else if [ToStage] = "Complete" then 9
          else if [ToStage] = "Blocked" then 98
          else if [ToStage] = "Removed" then 99
          else 999,
        Int64.Type
      ),

  WithDebug =
    Table.AddColumn(
        AddStageOrder,
        "DurationDebug",
        each Text.From([DurationDays]) & "d / "
           & Text.From([BusinessDays]) & "bd / "
           & Text.From([BusinessHours]) & "h",
        type text
    ),

  TypedFinal =
      Table.TransformColumnTypes(
        WithDebug,
        {
          {"OpportunityId", type text},
          {"FromStage", type text}, {"ToStage", type text}, {"NextStage", type text},
          {"StageStart", type datetime}, {"StageEnd", type nullable datetime},
          {"DurationDays", Int64.Type}, {"BusinessDays", Int64.Type}, {"BusinessHours", type number},
          {"Opp_Name", type text}, {"Opp_StageName", type text}, {"Opp_CloseDate", type nullable date},
          {"Account_Name", type text},
          {"Owner_Name", type text},
          {"HistUser_Name", type text},
          {"IsOpenStage", type logical},
          {"StageOrder", Int64.Type}
        }
      )
in
  TypedFinal
