import time
import csv
from neo4j import GraphDatabase

# Lista delle query che vogliamo testare
query_list = [
    # Query 1: direttori con nazionalità diversa dalla sede centrale
    """MATCH (p:Person)-[d:DIRECTOR_OF]->(c:Company)
    WHERE p.nationality <> c.HQ_country
    RETURN 
      p.first_name + " " + p.last_name AS Full_name,
      c.company_name AS Company,
      d.join_date AS Join_date,
      p.nationality AS DirectorNationality,
      c.HQ_country AS CompanyNationality;""",

    # Query 2: direttori che hanno anche quote in aziende con sede diversa
    """
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
      s.share_id as share_id
    """,

    # Query 3: aziende con sede e paese legale diversi e conti esteri
    """
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
    """,

    # Query 4: persone che possiedono almeno il 1.5% e fanno transazioni alte
    """
    MATCH (p:Person)-[:OWNS]->(s:Share)-[:IS_PART]->(c:Company)
    WHERE s.percentage >= 1.50
    MATCH (p)-[:HAS_ACCOUNT]->(a:Account)-[:SENT]->(t:Transaction)
    WHERE t.amount > 10000
    RETURN DISTINCT p.first_name + ' ' + p.last_name AS FullName,
           c.company_name AS Company,
           s.percentage AS Ownership,
           t.amount AS TransactionAmount,
           t.transaction_date AS Date
    ORDER BY t.amount DESC
    """
]

# Connessione al database Neo4j
uri = "neo4j://localhost:7687"
auth_credentials = ("neo4j", "password")

# Funzione che esegue una query e registra quanto tempo ci mette
def execute_query(query, query_num):
    filename = f"query_{query_num}_timer.csv"  # nome file CSV per salvare i risultati
    times = []  # lista dei tempi per ogni esecuzione

    # Eseguo la stessa query 31 volte (per avere una media affidabile)
    for i in range(31):
        print(f"Esecuzione {i + 1} per la query {query_num}")

        # Apro la connessione a Neo4j
        driver = GraphDatabase.driver(uri, auth=auth_credentials)
        try:
            with driver.session() as session:
                start_time = time.perf_counter()  # tempo prima della query

                # qui eseguo la query e consumo tutti i risultati
                result = session.run(query)
                list(result)  # così Neo4j è costretto a completare la query

                end_time = time.perf_counter()  # tempo dopo la query

                # Calcolo il tempo impiegato in millisecondi
                execution_time = (end_time - start_time) * 1000
                times.append(execution_time)
        except Exception as e:
            print(f"Errore durante l'esecuzione della query {query_num}: {e}")
            times.append(float('nan'))  # se c'è un errore, salvo NaN (not a number)
        finally:
            driver.close()  # chiudo la connessione

    # Scrivo i tempi in un file CSV
    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Run", "Execution Time"])
        for i, exec_time in enumerate(times):
            writer.writerow([i + 1, exec_time])

        # Calcolo la media dei tempi (escludendo la prima esecuzione e gli errori)
        valid_times = [t for t in times[1:] if not (t != t)]  # escludo i NaN
        if valid_times:
            average_time = sum(valid_times) / len(valid_times)
            writer.writerow(["Media", average_time])
        else:
            writer.writerow(["Media", "N/A"])

# Eseguo tutte le query una alla volta
for i, query in enumerate(query_list):
    execute_query(query, i + 1)

print("Script completato. I tempi di esecuzione sono stati salvati nei file CSV.")
