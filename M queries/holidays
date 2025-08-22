let
    // ---- Settings
    StartYear = 2024,
    EndYear   = 2026,
    Years     = {StartYear..EndYear},

    // ---- Helpers
    // weekday index uses Monday=0 ... Sunday=6
    NthWeekdayOfMonth = (y as number, m as number, n as number, weekday as number) as date =>
        let
            first  = #date(y,m,1),
            offset = Number.Mod(weekday - Date.DayOfWeek(first, Day.Monday) + 7, 7),
            d      = Date.AddDays(first, offset + 7 * (n - 1))
        in  d,

    LastWeekdayOfMonth = (y as number, m as number, weekday as number) as date =>
        let
            last   = Date.EndOfMonth(#date(y,m,1)),
            offset = Number.Mod(Date.DayOfWeek(last, Day.Monday) - weekday + 7, 7),
            d      = Date.AddDays(last, -offset)
        in  d,

    Observed = (d as date) as date =>
        let
            dow = Date.DayOfWeek(d, Day.Monday),
            // Sat -> Fri, Sun -> Mon, else same day
            obs = if dow = 5 then Date.AddDays(d, -1)
                  else if dow = 6 then Date.AddDays(d,  1)
                  else d
        in  obs,

    // ---- Build one year's holidays
    MakeYear = (y as number) as list =>
        {
            [Year=y, Name="New Year's Day",                                  Date=#date(y,1,1),  ObservedDate=Observed(#date(y,1,1))],
            [Year=y, Name="Martin Luther King Jr. Day",                      Date=NthWeekdayOfMonth(y,1,3,0),   ObservedDate=NthWeekdayOfMonth(y,1,3,0)],
            [Year=y, Name="Presidents' Day",                                 Date=NthWeekdayOfMonth(y,2,3,0),   ObservedDate=NthWeekdayOfMonth(y,2,3,0)],
            [Year=y, Name="Memorial Day",                                    Date=LastWeekdayOfMonth(y,5,0),    ObservedDate=LastWeekdayOfMonth(y,5,0)],
            [Year=y, Name="Juneteenth National Independence Day",            Date=#date(y,6,19),  ObservedDate=Observed(#date(y,6,19))],
            [Year=y, Name="Independence Day",                                Date=#date(y,7,4),   ObservedDate=Observed(#date(y,7,4))],
            [Year=y, Name="Labor Day",                                       Date=NthWeekdayOfMonth(y,9,1,0),   ObservedDate=NthWeekdayOfMonth(y,9,1,0)],
            [Year=y, Name="Columbus Day / Indigenous Peoples' Day",          Date=NthWeekdayOfMonth(y,10,2,0),  ObservedDate=NthWeekdayOfMonth(y,10,2,0)],
            [Year=y, Name="Veterans Day",                                    Date=#date(y,11,11), ObservedDate=Observed(#date(y,11,11))],
            [Year=y, Name="Thanksgiving Day",                                Date=NthWeekdayOfMonth(y,11,4,3),  ObservedDate=NthWeekdayOfMonth(y,11,4,3)],
            [Year=y, Name="Christmas Day",                                   Date=#date(y,12,25), ObservedDate=Observed(#date(y,12,25))]
        },

    // ---- Combine years and shape table
    Records   = List.Combine(List.Transform(Years, each MakeYear(_))),
    Holidays0 = Table.FromRecords(Records),
    WithMonth = Table.AddColumn(Holidays0, "Month", each Date.Month([Date]), Int64.Type),
    WithDay   = Table.AddColumn(WithMonth, "Day",   each Date.Day([Date]),   Int64.Type),
    WithShift = Table.AddColumn(WithDay, "ObservedShiftDays", each Duration.Days([ObservedDate] - [Date]), Int64.Type),
    Sorted    = Table.Sort(WithShift, {{"Date", Order.Ascending}, {"Name", Order.Ascending}})
in
    Sorted
