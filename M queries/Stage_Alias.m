// ============================================================================
// STAGE_ALIAS  (Canonical → Order only; avoid "Source" step name)
// ============================================================================
let
    AliasRows = {
        {"New",                  10},
        {"Staging Engagement",   20},
        {"Carrier Engagement",   30},
        {"Engagement",           35},
        {"Ready to Assign",      40},
        {"Development",          50},
        {"Testing",              60},
        {"Pending Deployment",   70},
        {"Actual Go Live",       80},
        {"Post Go Live",         90},
        {"Complete",            100},
        {"Blocked",             120},
        {"Removed",             130}
    },
    Stage_Alias_Table =
        Table.FromRows(AliasRows, {"CanonicalStage","StageOrder"})
in
    Stage_Alias_Table