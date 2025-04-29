
// indici per le query
db.persons.createIndex({ pers_id: 1 }, { unique: true });
db.companies.createIndex({ comp_id: 1 }, { unique: true });
db.accounts.createIndex({ acc_id: 1 }, { unique: true });
db.transactions.createIndex({ trans_id: 1 }, { unique: true });
db.shares.createIndex({ share_id: 1 }, { unique: true });


// query 1  ok
  db.directors.aggregate([
    {
      $lookup: {
        from: "persons",
        localField: "pers_id",
        foreignField: "pers_id",
        as: "person"
      }
    },
    { $unwind: "$person" },
    {
      $lookup: {
        from: "companies",
        localField: "comp_id",
        foreignField: "comp_id",
        as: "company"
      }
    },
    { $unwind: "$company" },
    {
      $match: {
        $expr: { $ne: ["$person.nationality", "$company.HQ_country"] }
      }
    },
    {
      $project: {
        Full_name: { $concat: ["$person.first_name", " ", "$person.last_name"] },
        Company: "$company.company_name",
        Join_date: "$join_date",
        DirectorNationality: "$person.nationality",
        CompanyNationality: "$company.HQ_country"
      }
    }
  ]);
  




//query 2 ok


db.directors.aggregate([
  {
    $lookup: {
      from: "persons",
      localField: "pers_id",
      foreignField: "pers_id",
      as: "person"
    }
  },
  { $unwind: "$person" },
  {
    $lookup: {
      from: "companies",
      localField: "comp_id",
      foreignField: "comp_id",
      as: "company"
    }
  },
  { $unwind: "$company" },
  {
    $lookup: {
      from: "shares",
      localField: "person.pers_id",
      foreignField: "owner_id",
      as: "shares"
    }
  },
  { $unwind: "$shares" },
  {
    $lookup: {
      from: "companies",
      localField: "shares.comp_id",
      foreignField: "comp_id",
      as: "owned_company"
    }
  },
  { $unwind: "$owned_company" },
  {
    $match: {
      $expr: {
        $and: [
          { $ne: ["$person.nationality", "$company.HQ_country"] },
          { $ne: ["$company.HQ_country", "$owned_company.HQ_country"] }
        ]
      }
    }
  },
  {
    $project: {
      Full_name: { $concat: ["$person.first_name", " ", "$person.last_name"] },
      Company: "$company.company_name",
      Join_date: "$join_date",
      DirectorNationality: "$person.nationality",
      CompanyNationality: "$company.HQ_country",
      Percentage: "$shares.percentage",
      owned_companyName: "$owned_company.company_name",
      share_id: "$shares.share_id"
    }
  }
]);


//query3 ok

db.companies.aggregate([
  // 1) Solo aziende con HQ ≠ legal
  {
    $match: {
      $expr: { $ne: ["$HQ_country", "$legal_country"] }
    }
  },

  // 2) Join sugli account
  {
    $lookup: {
      from: "account",        // collection corretta
      localField: "comp_id",
      foreignField: "comp_id",
      as: "accounts"
    }
  },

  // 3) Sgombera gli array vuoti
  { $unwind: "$accounts" },

  // 4) Filtra solo account in paesi diversi da HQ e legal
  {
    $match: {
      $expr: {
        $and: [
          { $ne: ["$accounts.bank_country", "$HQ_country"] },
          { $ne: ["$accounts.bank_country", "$legal_country"] }
        ]
      }
    }
  },

  // 5) Raggruppa per azienda, contando e raccogliendo i paesi
  {
    $group: {
      _id: "$company_name",
      LegalCountry:        { $first: "$legal_country" },
      HQCountry:           { $first: "$HQ_country" },
      ForeignBankCountries:{ $addToSet: "$accounts.bank_country" },
      ForeignAccounts:     { $sum: 1 }
    }
  },

  // 6) Ordina per numero di account esteri
  {
    $sort: { ForeignAccounts: -1 }
  }
]);





// query 4

db.shares.aggregate([
  {
    $match: { percentage: { $gte: 10 } }
  },
  {
    $lookup: {
      from: "persons",
      localField: "owner_id",
      foreignField: "pers_id",
      as: "person"
    }
  },
  { $unwind: "$person" },
  {
    $lookup: {
      from: "companies",
      localField: "comp_id",
      foreignField: "comp_id",
      as: "company"
    }
  },
  { $unwind: "$company" },
  {
    $lookup: {
      from: "account",
      localField: "owner_id",
      foreignField: "owner_id",
      as: "accounts"
    }
  },
  { $unwind: "$accounts" },
  {
    $lookup: {
      from: "transactions",
      localField: "accounts.acc_id",
      foreignField: "sender_id",
      as: "transactions"
    }
  },
  { $unwind: "$transactions" },
  {
    $match: { "transactions.amount": { $gt: 10000 } }
  },
  {
    $project: {
      FullName: { $concat: ["$person.first_name", " ", "$person.last_name"] },
      Company: "$company.company_name",
      Ownership: "$percentage",
      TransactionAmount: "$transactions.amount",
      Date: "$transactions.transaction_date"
    }
  },
  {
    $sort: { TransactionAmount: -1 }
  }
]);
