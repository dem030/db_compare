// Creazione dei vincoli di unicità con la sintassi aggiornata


// Trovare direttori con nazionalità diversa dalla sede centrale dell'azienda
MATCH (p:Person)-[d:DIRECTOR_OF]->(c:Company)
WHERE p.nationality <> c.HQ_country
RETURN 
  p.first_name + " " + p.last_name AS Full_name,
  c.company_name AS Company,
  d.join_date AS Join_date,
  p.nationality AS DirectorNationality,
  c.HQ_country AS CompanyNationality;

// Trovare direttori con nazionalità diversa dalla banca delle aziende
MATCH (p:Person)-[d:DIRECTOR_OF]->(c:Company)
MATCH (p)-[o:OWNS]->(s:Share)-[:IS_PART]->(c2:Company)
WHERE p.nationality <> c.HQ_country and c.HQ_country <> c2.HQ_country
RETURN 
  p.first_name + " " + p.last_name AS Full_name,
  c.company_name AS Company,
  d.join_date AS Join_date,
  p.nationality AS DirectorNationality,
  c.HQ_country AS CompanyNationality,
  sum(s.percentage) as Percentage,
  c2.company_name as owned_companyName,
  s.share_id as share_id;
  


  

// Raggruppare per azienda e persona sommandone le percentuali
MATCH (c:Company)-[:CHAS_ACCOUNT]->(a:Account)
WHERE c.HQ_country <> c.legal_country
  AND a.bank_country <> c.HQ_country
  AND a.bank_country <> c.legal_country
RETURN c.company_name AS Company,
       c.legal_country AS LegalCountry,
       c.HQ_country AS HQCountry,
       COLLECT(DISTINCT a.bank_country) AS ForeignBankCountries,
       COUNT(a) AS ForeignAccounts
ORDER BY ForeignAccounts DESC


MATCH (p:Person)-[:OWNS]->(s:Share)-[:IS_PART]->(c:Company)
WHERE s.percentage >= 50
MATCH (p)-[:HAS_ACCOUNT]->(a:Account)-[:SENT]->(t:Transaction)
WHERE t.amount > 10000
RETURN DISTINCT p.first_name + ' ' + p.last_name AS FullName,
       c.company_name AS Company,
       s.percentage AS Ownership,
       t.amount AS TransactionAmount,
       t.transaction_date AS Date
ORDER BY t.amount DESC

