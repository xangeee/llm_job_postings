import logging
import os

from neo4j import GraphDatabase
from retry import retry

COMPANY_CSV_PATH = os.getenv("COMPANY_CSV_PATH")
JOB_POSTINGS_CSV_PATH = os.getenv("JOB_POSTINGS_CSV_PATH")
    
EMPLOYEES_CSV_PATH = os.getenv("EMPLOYEES_CSV_PATH")
LOCATION_JOB_CSV_PATH = os.getenv("LOCATION_JOB_CSV_PATH")
LOCATION_COMPANY_CSV_PATH = os.getenv("LOCATION_COMPANY_CSV_PATH")
SALARY_RANGE_CSV_PATH = os.getenv("SALARY_RANGE_CSV_PATH")


NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

LOGGER = logging.getLogger(__name__)

NODES = ["Job_posting", "Company", "Employee", "Location", "Salary_range"]


def _set_uniqueness_constraints(tx, node):
    query = f"""CREATE CONSTRAINT IF NOT EXISTS FOR (n:{node})
        REQUIRE n.id IS UNIQUE;"""
    _ = tx.run(query, {})


@retry(tries=100, delay=10)
def load_jobs_graph_from_csv() -> None:
    """Load structured hospital CSV data following
    a specific ontology into Neo4j"""

    driver = GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
    )

    LOGGER.info("Setting uniqueness constraints on nodes")
    with driver.session(database="neo4j") as session:
        for node in NODES:
            session.execute_write(_set_uniqueness_constraints, node)


    LOGGER.info("Loading company nodes")
    with driver.session(database="neo4j") as session:
        query = f"""
        LOAD CSV WITH HEADERS
        FROM '{COMPANY_CSV_PATH}' AS companies
        MERGE (c:Company {{id: toInteger(companies.company_id),
                            name: companies.name,
                            speciality: companies.speciality,
                            description: companies.description,
                            company_size: toInteger(companies.company_size),
                            url: companies.url
                            }});
        """
        _ = session.run(query, {})

    LOGGER.info("Loading employee nodes")
    with driver.session(database="neo4j") as session:
        query = f"""
        LOAD CSV WITH HEADERS
        FROM '{EMPLOYEES_CSV_PATH}' AS employees
        MERGE (e:Employee {{id: toInteger(employees.id),
                            employee_count: toInteger(employees.employee_count),
                            follower_count: toInteger(employees.follower_count)
                            }});
        """
        _ = session.run(query, {})


    # LOGGER.info("Loading company location nodes")
    # with driver.session(database="neo4j") as session:
    #     query = f"""
    #     LOAD CSV WITH HEADERS
    #     FROM '{LOCATION_COMPANY_CSV_PATH}' AS locations
    #     MERGE (l:Location {{id: toInteger(locations.id),
    #                         industry: locations.industry,
    #                         country: locations.country,
    #                         city: locations.city,
    #                         zip_code: locations.zip_code,
    #                         address: locations.address
    #                         }});
    #     """
    #     _ = session.run(query, {})


    LOGGER.info("Loading job location nodes")
    with driver.session(database="neo4j") as session:
        query = f"""
        LOAD CSV WITH HEADERS
        FROM '{LOCATION_JOB_CSV_PATH}' AS locations
        MERGE (l:Location {{id: toInteger(locations.id),
                            location: locations.location,
                            zip_code: locations.zip_code,
                            industry: locations.industry
                            }});
        """
        _ = session.run(query, {})


    LOGGER.info("Loading jobs nodes")
    with driver.session(database="neo4j") as session:
        query = f"""
        LOAD CSV WITH HEADERS FROM '{JOB_POSTINGS_CSV_PATH}' AS jobs
        MERGE (j:Job_posting {{id: toInteger(jobs.job_id),
                            title:jobs.title,
                            description: jobs.description,
                            views: toInteger(jobs.views),
                            job_posting_url: jobs.job_posting_url,
                            application_url: jobs.application_url,
                            application_type:jobs.application_type,
                            sponsored:jobs.sponsored,
                            skills:jobs.skills

        }})
            ON CREATE SET j.formatted_experience_level = jobs.formatted_experience_level
            ON MATCH SET j.formatted_experience_level = jobs.formatted_experience_level
            ON CREATE SET j.formatted_work_type = jobs.formatted_work_type
            ON MATCH SET j.formatted_work_type = jobs.formatted_work_type
            ON CREATE SET j.work_type = jobs.work_type
            ON MATCH SET j.work_type = jobs.work_type
            ON CREATE SET j.original_listed_time = jobs.original_listed_time
            ON MATCH SET j.original_listed_time = jobs.original_listed_time
            ON CREATE SET j.expiry = jobs.expiry
            ON MATCH SET j.expiry = jobs.expiry
         """
        _ = session.run(query, {})


    LOGGER.info("Loading salary range nodes")
    with driver.session(database="neo4j") as session:
        query = f"""
        LOAD CSV WITH HEADERS
        FROM '{SALARY_RANGE_CSV_PATH}' AS salary_ranges
        MERGE (s:Salary_range {{id: toInteger(salary_ranges.salary_id),
                        max_salary: toFloat(salary_ranges.max_salary),
                        med_salary: toFloat(salary_ranges.med_salary),
                        min_salary: toFloat(salary_ranges.min_salary),
                        pay_period: salary_ranges.pay_period,
                        currency: salary_ranges.currency,
                        compensation_type: salary_ranges.compensation_type,
                        benefits: salary_ranges.benefits
                        }});
        """
        _ = session.run(query, {})

    LOGGER.info("Loading 'HAS' relationships")
    with driver.session(database="neo4j") as session:
        query = f"""
        LOAD CSV WITH HEADERS FROM '{JOB_POSTINGS_CSV_PATH}' AS row
        MATCH (source: `Job_posting` {{ `id`: toInteger(trim(row.`job_id`)) }})
        MATCH (target: `Salary_range` {{ `id`:toInteger(trim(row.`salary_id`))}})
        MERGE (source)-[has: `HAS`]->(target)
        """
        _ = session.run(query, {})

    LOGGER.info("Loading 'located_in' relationships")
    with driver.session(database="neo4j") as session:
        query = f"""
        LOAD CSV WITH HEADERS FROM '{JOB_POSTINGS_CSV_PATH}' AS row
            MATCH (v:Job_posting {{id: toInteger(row.job_id)}})
            MATCH (r:Location {{id: toInteger(row.location_id)}})
            MERGE (v)-[located_in:LOCATED_IN]->(r)
        """
        _ = session.run(query, {})


    # LOGGER.info("Loading 'located_in' relationships")
    # with driver.session(database="neo4j") as session:
    #     query = f"""
    #     LOAD CSV WITH HEADERS FROM '{COMPANY_CSV_PATH}' AS row
    #         MATCH (v:Company {{id: toInteger(row.company_id)}})
    #         MATCH (r:Location {{id: toInteger(row.location_id)}})
    #         MERGE (v)-[located_in:LOCATED_IN]->(r)
    #     """
    #     _ = session.run(query, {})



    LOGGER.info("Loading 'PUBLISHES' relationships")
    with driver.session(database="neo4j") as session:
        query = f"""
        LOAD CSV WITH HEADERS FROM '{JOB_POSTINGS_CSV_PATH}' AS row
            MATCH (v:Job_posting {{id: toInteger(row.job_id)}})
            MATCH (p:Company{{id: toInteger(row.company_id)}})
            MERGE (v)<-[publises:PUBLISHES]-(p)
            ON CREATE SET
                publises.original_listed_time = row.original_listed_time,
                publises.expiry = row.expiry
        """
        _ = session.run(query, {})


    LOGGER.info("Loading 'HIRES' relationships")
    with driver.session(database="neo4j") as session:
        query = f"""
        LOAD CSV WITH HEADERS FROM '{EMPLOYEES_CSV_PATH}' AS row
            MATCH (h:Company {{id: toInteger(row.company_id)}})
            MATCH (p:Employee {{id: toInteger(row.id)}})
            MERGE (h)-[hires:HIRES]->(p)
            ON CREATE SET
                hires.time_recorded = row.time_recorded
        """
        _ = session.run(query, {})


if __name__ == "__main__":
    load_jobs_graph_from_csv()
