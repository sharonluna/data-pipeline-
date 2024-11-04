import re
from os import path
from bs4 import BeautifulSoup
import pandas as pd
import requests

# DIRECTORIES
DIR = path.dirname(__file__)
DATA_DIRECTORY = path.abspath(path.join(DIR, "../data"))

# INPUT FILES
GEO_CATALOG_FILE = path.join(DATA_DIRECTORY, "Catálogo de relación geográfica.csv")

# OUTPUT FILES
COUNTRY_POPULATION_FILE = path.join(DATA_DIRECTORY, "country_population.csv")

# UNIQUE COUNTRIES LIST
GEO_CATALOG = pd.read_csv(GEO_CATALOG_FILE)
UNIQUE_COUNTRIES = [country.strip().lower() for country in GEO_CATALOG["País"].unique()]


# AUXILIARY FUNCTIONS
def clean_country_name(country_text: str) -> str:
    """Function to clean country names

    Args:
        country_text (str): country name

    Returns:
        str: cleaned country name
    """

    country = country_text.split("\xa0")[0]
    country = re.sub(r"\[.*?\]|\d+", "", country).strip()
    country = country.replace("\u200b", "")
    words = country.split()
    country_cleaned = " ".join(
        dict.fromkeys(words)
    )  # Preserve original order, remove duplicates

    return country_cleaned


def scrape_population():
    """
    Scrapes population data from a specified Wikipedia page and returns it as a
    pandas DataFrame.

    Returns:
        pd.DataFrame:
            A DataFrame containing the population data with columns "País" (country) and "Población" (population).
    Raises:
        requests.exceptions.RequestException: If there is an issue with the network request.
    """
    url = "https://es.wikipedia.org/wiki/Anexo:Países_y_territorios_dependientes_por_densidad_de_población"
    response = requests.get(url, timeout=60)
    if not response.ok:
        raise RuntimeError(
            f"Error: Received error from {url} with status code {response.status_code}"
        )

    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", {"class": "wikitable"})
    if not table:
        raise RuntimeError("No table found on the Wikipedia page.")

    population_data = {}

    # Iterate table rows
    for row in table.find_all("tr"):
        columns = row.find_all("td")

        # Skip rows with too few columns
        if len(columns) < 4:
            continue

        # Get and clean country name
        country = columns[1].text.strip().lower()
        country = clean_country_name(country)

        population_text = (
            columns[3]
            .text.strip()
            .replace(".", "")
            .replace(",", "")
            .replace("\xa0", "")
        )
        # Convert population to integer
        try:
            population = int(population_text)
        except ValueError:
            population = None

        # Search for countries included in file
        if country in UNIQUE_COUNTRIES:
            population_data[country.title()] = population

    # Convert dictionary to DataFrame
    return pd.DataFrame(list(population_data.items()), columns=["País", "Población"])


if __name__ == "__main__":
    # Scrape population data
    population_df = scrape_population()

    # Display population data for verification
    print(population_df)

    # Save population data to CSV file
    population_df.to_csv(COUNTRY_POPULATION_FILE, index=False)
    print(f"Population data saved to {COUNTRY_POPULATION_FILE}")
