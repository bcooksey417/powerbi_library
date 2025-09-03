let
    // PARAMETERS
    StartYear = 2019,
    EndYear   = Date.Year(DateTime.LocalNow()) + 1,

    // DATE RANGE
    StartDate = #date(StartYear, 1, 1),
    EndDate   = #date(EndYear, 12, 31),
    DateList  = List.Dates(StartDate, Duration.Days(EndDate - StartDate) + 1, #duration(1,0,0,0)),

    // BASE TABLE
    DateTable = Table.FromList(DateList, Splitter.SplitByNothing(), {"Date"}, null, ExtraValues.Error),
    ChangedType = Table.TransformColumnTypes(DateTable, {{"Date", type date}}),

    // STANDARD CALENDAR COLUMNS
    AddYear        = Table.AddColumn(ChangedType, "Year", each Date.Year([Date]), Int64.Type),
    AddQuarter     = Table.AddColumn(AddYear, "Quarter", each "Q" & Number.ToText(Date.QuarterOfYear([Date])), type text),
    AddMonthNum    = Table.AddColumn(AddQuarter, "Month Number", each Date.Month([Date]), Int64.Type),
    AddMonthName   = Table.AddColumn(AddMonthNum, "Month Name", each Date.MonthName([Date]), type text),
    AddMonthShort  = Table.AddColumn(AddMonthName, "Month Short", each Text.Start(Date.MonthName([Date]),3), type text),
    AddDay         = Table.AddColumn(AddMonthShort, "Day", each Date.Day([Date]), Int64.Type),
    AddDow         = Table.AddColumn(AddDay, "Day of Week", each Date.DayOfWeek([Date], Day.Monday), Int64.Type), // 0=Mon ... 6=Sun
    AddDowName     = Table.AddColumn(AddDow, "Day Name", each Date.DayOfWeekName([Date]), type text),
    AddWeekNum     = Table.AddColumn(AddDowName, "Week Number", each Date.WeekOfYear([Date]), Int64.Type),
    AddYearMonth   = Table.AddColumn(AddWeekNum, "Year-Month", each Text.From(Date.Year([Date])) & "-" & Text.PadStart(Text.From(Date.Month([Date])),2,"0"), type text),

    // FISCAL (starts July=7; change if needed)
    FiscalStartMonth = 7,
    AddFiscalYear =
        Table.AddColumn(AddYearMonth, "Fiscal Year",
            each if Date.Month([Date]) >= FiscalStartMonth then Date.Year([Date]) + 1 else Date.Year([Date]),
            Int64.Type
        ),
    AddFiscalQuarter =
        Table.AddColumn(AddFiscalYear, "Fiscal Quarter",
            each "FQ" & Number.ToText(
                Number.IntegerDivide(
                    Number.Mod(Date.Month([Date]) - FiscalStartMonth + 12, 12), 3
                ) + 1
            ),
            type text
        ),

    // HOLIDAY LOOKUP (join to Holidays table by ObservedDate)
    Merged        = Table.NestedJoin(AddFiscalQuarter, {"Date"}, Holidays, {"ObservedDate"}, "HolidayJoin", JoinKind.LeftOuter),
    Expanded      = Table.ExpandTableColumn(Merged, "HolidayJoin", {"Name"}, {"HolidayName"}),
    WithIsHoliday = Table.AddColumn(Expanded, "IsHoliday", each [HolidayName] <> null, type logical),

    // BUSINESS DAY FLAG: Mon–Fri (0..4) and not a holiday
    WithBusiness  = Table.AddColumn(
                        WithIsHoliday,
                        "IsBusinessDay",
                        each ([Day of Week] >= 0 and [Day of Week] <= 4) and (not [IsHoliday]),
                        type logical
                    ),

    // FINAL ORDER
    Reordered =
        Table.ReorderColumns(
            WithBusiness,
            {"Date","Year","Quarter","Month Number","Month Name","Month Short","Year-Month",
             "Fiscal Year","Fiscal Quarter","Week Number","Day","Day of Week","Day Name",
             "IsHoliday","HolidayName","IsBusinessDay"}
        )
in
    Reordered