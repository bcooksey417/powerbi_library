let
    Source = Salesforce.Data("https://login.salesforce.com/", [ApiVersion=48]){[Name="Opportunity"]}[Data]
in
    Source