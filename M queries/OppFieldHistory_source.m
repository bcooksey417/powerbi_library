let
    Source = Salesforce.Data("https://login.salesforce.com/", [ApiVersion=48]){[Name="OpportunityFieldHistory"]}[Data]
in
    Source