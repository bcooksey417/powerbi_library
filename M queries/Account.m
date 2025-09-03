let
    Source = Salesforce.Data("https://login.salesforce.com/", [ApiVersion=48]),
    Account1 = Source{[Name="Account"]}[Data]
in
    Account1