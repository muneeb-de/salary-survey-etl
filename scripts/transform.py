import pandas as pd
import logging


class SalarySurveyTransformer:
    """
    Handles data transformation and cleaning for salary survey data.
    """

    def __init__(self):
        """Initialize transformer with logging."""
        self.logger = logging.getLogger("salary-survey-transform")

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rename dataframe columns to more readable and standardized names.
        
        Args:
            df: Input DataFrame with original column names
            
        Returns:
            DataFrame with renamed columns
        """
        try:
            self.logger.info("Starting _rename_columns()")
            rename_map = {
                'Timestamp': 'Timestamp',
                'How old are you?': 'Age',
                'What industry do you work in?': 'Industry',
                'Job title': 'Job_Title',
                'If your job title needs additional context, please clarify here:': 'Job_Context',
                "What is your annual salary? (You'll indicate the currency in a later question. If you are part-time or hourly, please enter an annualized equivalent -- what you would earn if you worked the job 40 hours a week, 52 weeks a year.)": 'Annual_Salary',
                'How much additional monetary compensation do you get, if any (for example, bonuses or overtime in an average year)? Please only include monetary compensation here, not the value of benefits.': 'Additional_Comp',
                'Please indicate the currency': 'Currency',
                'If "Other," please indicate the currency here: ': 'Currency_Other',
                'If your income needs additional context, please provide it here:': 'Income_Context',
                'What country do you work in?': 'Country',
                "If you're in the U.S., what state do you work in?": 'State',
                'What city do you work in?': 'City',
                'How many years of professional work experience do you have overall?': 'Experience_Overall',
                'How many years of professional work experience do you have in your field?': 'Experience',
                'What is your highest level of education completed?': 'Education',
                'What is your gender?': 'Gender',
                'What is your race? (Choose all that apply.)': 'Race',
                'Seniority': 'Seniority'
            }
            df.rename(columns=rename_map, inplace=True)
            return df
        except Exception as e:
            self.logger.critical(f"Exception occurred in _rename_columns() : {e}")
            return pd.DataFrame()

    def _normalize_job_titles(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize job titles by converting to lowercase, removing punctuation,
        and extracting seniority levels from job titles.
        
        Args:
            df: Input DataFrame with Job_Title column
            
        Returns:
            DataFrame with normalized job titles and seniority levels
        """
        try:
            self.logger.info("Starting _normalize_job_titles()")
            
            # Clean and standardize job titles
            df['Job_Title'] = (
                df['Job_Title']
                .str.lower()
                .str.replace(r'[^\w\s]', '', regex=True)
                .str.replace(r'\s+', ' ', regex=True)
                .str.strip()
            )

            # Pattern to extract seniority levels from job titles
            pattern = r'\b(sr|senior|lead|principal|jr|junior|chief|director|manager|assistant|associate|head|coordinator|i{1,3}|iv|v)\b'

            # Extract and map seniority levels
            df['Seniority'] = df['Job_Title'].str.findall(pattern).str[0]
            df['Seniority'] = df['Seniority'].str.lower().map({
                'sr': 'Senior',
                'senior': 'Senior',
                'lead': 'Lead',
                'principal': 'Principal',
                'chief': 'Executive',
                'director': 'Executive',
                'manager': 'Manager',
                'assistant': 'Assistant',
                'associate': 'Associate',
                'head': 'Executive',
                'coordinator': 'Coordinator',
                'jr': 'Junior',
                'junior': 'Junior',
                'i': 'Level I',
                'ii': 'Level II',
                'iii': 'Level III',
                'iv': 'Level IV',
                'v': 'Level V'
            })
            
            # Clean seniority column
            df['Seniority'] = df['Seniority'].map(lambda x: None if pd.isna(x) else x)
            df['Seniority'] = df['Seniority'].astype("string").str.slice(0, 100)

            return df
        except Exception as e:
            self.logger.critical(f"Exception occurred in _normalize_job_titles() {e}")
            return pd.DataFrame()

    def _standardize_annual_salaries(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize annual salary data by removing non-numeric characters.
        
        Args:
            df: Input DataFrame with Annual_Salary column
            
        Returns:
            DataFrame with cleaned annual salary data
        """
        try:
            self.logger.info("Starting _standardize_annual_salaries()")
            df['Annual_Salary'] = (
                df['Annual_Salary']
                .str.replace(r'[^0-9.]', '', regex=True)
                .replace(r'\s+', ' ', regex=True)
            )
            return df
        except Exception as e:
            self.logger.critical(f"Exception occurred in _standardize_annual_salaries() : {e}")
            return pd.DataFrame()

    def _standardize_additional_comp(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize additional compensation data.
        
        Args:
            df: Input DataFrame with Additional_Comp column
            
        Returns:
            DataFrame with cleaned additional compensation data
        """
        try:
            self.logger.info("Starting _standardize_additional_comp()")
            df['Additional_Comp'] = (
                df['Additional_Comp']
                .astype(str)
                .str.replace(r'[^0-9.]', '', regex=True)
                .replace(r'\s+', ' ', regex=True)
                .replace('', None)
            )
            return df
        except Exception as e:
            self.logger.critical(f"Exception occurred in _standardize_additional_comp() : {e}")
            return pd.DataFrame()

    def _standardize_currency_other_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize currency values and merge Currency_Other into Currency column.
        
        Args:
            df: Input DataFrame with Currency and Currency_Other columns
            
        Returns:
            DataFrame with standardized currency values and removed Currency_Other column
        """
        try:
            self.logger.info("Starting _standardize_currency_other_cols()")

            # Merge 'Other' currency values from Currency_Other into Currency
            df['Currency'] = df['Currency'].where(
                ~df['Currency'].str.lower().str.contains('other', na=False),
                df['Currency_Other']
            )
            
            # Clean and standardize currency values
            df['Currency'] = (
                df["Currency"]
                .str.upper()
                .str.replace(r"\(.*?\)", "", regex=True)
                .str.replace(r"[^A-Z/]", " ", regex=True)
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
            )

            # Map various currency representations to standardized codes
            replace_map = {
                "US DOLLAR": "USD",
                "AMERICAN DOLLARS": "USD",
                "USD": "USD",
                "BR": "BRL",
                "BRL": "BRL",
                "BRL R": "BRL",
                "INDIAN RUPEES": "INR",
                "RUPEES": "INR",
                "INR INDIAN RUPEE": "INR",
                "RMB": "CNY",
                "CHINA RMB": "CNY",
                "RMB CHINESE YUAN": "CNY",
                "EURO": "EUR",
                "DANISH KRONER": "DKK",
                "NORWEGIAN KRONER": "NOK",
                "POLISH ZLOTY": "PLN",
                "POLISH ZWOTY": "PLN",
                "PLN ZWOTY": "PLN",
                "KOREAN WON": "KRW",
                "KRW KOREAN WON": "KRW",
                "NEW ISRAELI SHEKEL": "ILS",
                "NIS": "ILS",
                "NIS NEW ISRAELI SHEKEL": "ILS",
                "ILS SHEKEL": "ILS",
                "ISRAELI SHEKELS": "ILS",
                "ARGENTINIAN PESO": "ARS",
                "ARGENTINE PESO": "ARS",
                "PESO ARGENTINO": "ARS",
                "MEXICAN PESOS": "MXN",
                "PESOS COLOMBIANOS": "COP",
                "PHILIPPINE PESO": "PHP",
                "PHILIPPINE PESOS": "PHP",
                "PHP PHILIPPINE PESO": "PHP",
                "AUD AUSTRALIAN": "AUD",
                "AUSTRALIAN DOLLARS": "AUD",
                "AUD NZD": "AUD/NZD",
                "SINGAPORE DOLLARA": "SGD",
                "THAI BAHT": "THB",
                "THAI  BAHT": "THB",
                "MYR": "MYR",
                "RM": "MYR",
                "EQUITY": "EQUITY",
                "0": None
            }
            df["Currency"] = df["Currency"].replace(replace_map)

            # Drop the now-redundant Currency_Other column
            df = df.drop(columns=['Currency_Other'])

            return df
        except Exception as e:
            self.logger.critical("Exception occurred in _standardize_currency_other_cols()")
            return pd.DataFrame()

    def _change_time_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert timestamp to standardized format.
        
        Args:
            df: Input DataFrame with Timestamp column
            
        Returns:
            DataFrame with standardized timestamp format
        """
        try:
            self.logger.info("Starting _change_time_format()")

            # Convert to datetime and format
            df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce", utc=True)
            df["Timestamp"] = df["Timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

            return df
        except Exception as e:
            self.logger.critical(f"Exception occurred in _change_time_format() : {e}")
            return pd.DataFrame()

    def _standardize_income_context(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize income context field.
        
        Args:
            df: Input DataFrame with Income_Context column
            
        Returns:
            DataFrame with cleaned income context
        """
        try:
            self.logger.info("Starting _standardize_income_context()")
            df['Income_Context'] = (
                df['Income_Context']
                .fillna('')
                .str.lower()
                .str.strip()
                .str.replace(r'\s+', ' ', regex=True)
                .replace('', None)
            )
            return df
        except Exception as e:
            self.logger.critical(f"Exception occurred in _standardize_income_context()")
            return pd.DataFrame()  

    def _standardize_country(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize country names and handle various country name variations.
        
        Args:
            df: Input DataFrame with Country column
            
        Returns:
            DataFrame with standardized country names
        """
        try:
            # Clean country names
            df['Country'] = (
                df['Country']
                .str.lower()
                .str.replace(r'[^a-z\s]', '', regex=True)
                .str.replace(r"\s+", " ", regex=True)      
            )
            
            # Comprehensive country mapping for standardization
            country_map = {
                # US variants
                "us": "United States",
                "usa": "United States",
                "u s": "United States",
                "u s a": "United States",
                "united states": "United States",
                "united state": "United States",
                "america": "United States",
                "the united states": "United States",
                
                # UK variants
                "uk": "United Kingdom",
                "u k": "United Kingdom",
                "united kingdom": "United Kingdom",
                "england": "United Kingdom",
                "great britain": "United Kingdom",
                "britain": "United Kingdom",
                "scotland": "United Kingdom",
                "wales": "United Kingdom",
                
                # Other common countries
                "canada": "Canada",
                "australia": "Australia",
                "new zealand": "New Zealand",
                "mexico": "Mexico",
                "france": "France",
                "germany": "Germany",
                "netherlands": "Netherlands",
                "india": "India",
                "japan": "Japan",
                
                # Misc/Non-country entries
                "global": "Global",
                "worldwide": "Global",
                "california": "United States",
                "europe": "Europe",
                "africa": "Africa",
            }
            
            df['Country'] = df['Country'].str.lower().map(country_map).where(lambda x: x.notna(), None)
            return df
        except Exception as e:
            self.logger.critical(f"Exception occurred in _standardize_country() : {e}")
            return pd.DataFrame()

    def _standardize_job_context(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean job context field.
        
        Args:
            df: Input DataFrame with Job_Context column
            
        Returns:
            DataFrame with cleaned job context
        """
        try:
            self.logger.info("Starting _standardize_job_context()")
            df['Job_Context'] = df['Job_Context'].where(df['Job_Context'].notna(), None)
            return df
        except Exception as e:
            self.logger.critical(f"Exception occurred in _standardize_job_context() : {e}")
            return pd.DataFrame()

    def _standardize_gender(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize gender values to consistent categories.
        
        Args:
            df: Input DataFrame with Gender column
            
        Returns:
            DataFrame with standardized gender values
        """
        try:
            self.logger.info("Starting _standardize_gender()")
            
            # Clean empty strings
            df['Gender'] = df['Gender'].replace(r'^\s*$', pd.NA, regex=True)

            # Standardize gender categories
            df['Gender'] = df['Gender'].replace({
                'Man': 'Male',
                'Woman': 'Female',
                'Non-binary': 'Non-Binary',
                'Other or prefer not to answer': 'Prefer not to say',
                'Prefer not to answer': 'Prefer not to say',
                '0': 'Prefer not to say'
            })

            # Fill missing values
            df['Gender'] = df['Gender'].fillna('Prefer not to say')
            return df
        except Exception as e:
            self.logger.critical(f"Exception occurred in _standardize_gender() : {e}")
            return pd.DataFrame()

    def _standardize_race(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize race values to consistent categories.
        
        Args:
            df: Input DataFrame with Race column
            
        Returns:
            DataFrame with standardized race values
        """
        try:
            self.logger.info("Starting _standardize_race()")
            
            # Clean empty strings
            df['Race'] = df['Race'].replace(r'^\s*$', pd.NA, regex=True)
            
            # Standardize race categories
            df['Race'] = df['Race'].replace({
                'Another option not listed here or prefer not to answer': 'Other',
                '0': 'Other'
            }).str.title()
            
            # Fill missing values
            df['Race'] = df['Race'].fillna('Other')
            return df
        except Exception as e:
            self.logger.critical(f"Exception occurred in _standardize_race() : {e}")
            return pd.DataFrame()

    def _standardize_education(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize education levels to consistent categories.
        
        Args:
            df: Input DataFrame with Education column
            
        Returns:
            DataFrame with standardized education values
        """
        try:
            self.logger.info("Starting _standardize_education()")
            
            # Clean empty strings
            df['Education'] = df['Education'].replace(r'^\s*$', pd.NA, regex=True)

            # Standardize education categories
            df['Education'] = df['Education'].replace({
                "Master's degree": "Master's Degree",
                "College degree": "College Degree",
                "Some college": "Some College",
                "Professional degree (MD, JD, etc.)": "Professional Degree",
            })

            # Fill missing values
            df['Education'] = df['Education'].fillna('Prefer not to say')
            return df
        except Exception as e:
            self.logger.critical(f"Exception occurred in _standardize_education() : {e}")
            return pd.DataFrame()   

    def _standardize_experience(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize field experience to consistent range categories.
        
        Args:
            df: Input DataFrame with Experience column
            
        Returns:
            DataFrame with standardized experience ranges
        """
        try:
            self.logger.info("Starting _standardize_experience()")
            
            # Clean empty strings
            df['Experience'] = df['Experience'].replace(r'^\s*$', pd.NA, regex=True)
            df['Experience'] = df['Experience'].str.strip()
            
            # Standardize experience ranges
            df['Experience'] = df['Experience'].replace({
                '1 year or less': '0-1 years',
                '2 - 4 years': '2-4 years',
                '5-7 years': '5-7 years',
                '8 - 10 years': '8-10 years',
                '11 - 20 years': '11-20 years',
                '21 - 30 years': '21-30 years',
                '30 - 40 years': '30-40 years',
                '41 years or more': '41+ years'
            })
            
            # Fill missing values
            df['Experience'] = df['Experience'].fillna('0-1 years')
            return df
        except Exception as e:
            self.logger.critical(f"Exception occurred in _standardize_experience() : {e}")
            return pd.DataFrame()

    def _standardize_overall_experience(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize overall work experience to consistent range categories.
        
        Args:
            df: Input DataFrame with Experience_Overall column
            
        Returns:
            DataFrame with standardized overall experience ranges
        """
        try:
            self.logger.info("Starting _standardize_overall_experience()")
            
            # Clean empty strings
            df['Experience_Overall'] = df['Experience_Overall'].replace(r'^\s*$', pd.NA, regex=True)
            df['Experience_Overall'] = df['Experience_Overall'].str.strip()
            
            # Standardize experience ranges
            df['Experience_Overall'] = df['Experience_Overall'].replace({
                '1 year or less': '0-1 years',
                '2 - 4 years': '2-4 years',
                '5-7 years': '5-7 years',
                '8 - 10 years': '8-10 years',
                '11 - 20 years': '11-20 years',
                '21 - 30 years': '21-30 years',
                '31 - 40 years': '30-40 years',
                '41 years or more': '41+ years'
            })
            
            # Fill missing values
            df['Experience_Overall'] = df['Experience_Overall'].fillna('0-1 years')
            return df
        except Exception as e:
            self.logger.critical(f"Exception occurred in _standardize_overall_experience() : {e}")
            return pd.DataFrame()

    def _standardize_state(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize state/province values.
        
        Args:
            df: Input DataFrame with State column
            
        Returns:
            DataFrame with cleaned state values
        """
        try:
            self.logger.info("Starting _standardize_state()")
            df['State'] = (
                df['State']
                .astype(str)                             
                .str.split(',')                          # Handle comma-separated values
                .str[0]                                  # Take first part only
                .str.strip()                             # Remove whitespace
                .replace(['0', 'nan', 'NaN', 'None', '', 'null'], None)  # Clean invalid values
            )
            return df
        except Exception as e:
            self.logger.critical(f"Exception occurred in _standardize_state() : {e}")
            return pd.DataFrame()

    def _standardize_city(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize city names with comprehensive validation.
        
        Args:
            df: Input DataFrame with City column
            
        Returns:
            DataFrame with cleaned and validated city names
        """
        try:
            self.logger.info("Starting _standardize_city()")
            
            # Basic cleaning and encoding handling
            df['City'] = (
                df['City']
                .astype(str)
                .str.encode('latin1', errors='ignore')
                .str.decode('utf-8', errors='ignore')
                .str.strip()
                .str.replace(r'\s+', ' ', regex=True)
                .replace(['', 'nan', 'None', 'NaN'], None)
            )

            # List of invalid entries to filter out
            invalid_entries = [
                'n/a', 'na', 'none', 'no answer', 'prefer not to answer', 'undisclosed',
                'test', 'remote', 'work remotely', 'telecommute', 'virtual worker',
                'home worker', 'student', 'small city', 'stay', 'unknown', 'remove',
                'dhgbfv', 'pubw', 'ff', 's'
            ]

            # Remove invalid entries
            df['City'] = df['City'].apply(
                lambda x: None if isinstance(x, str) and x.lower().strip() in invalid_entries else x
            )

            # Standardize capitalization
            df['City'] = df['City'].apply(
                lambda x: x.title().strip() if isinstance(x, str) else x
            )

            return df
        except Exception as e:
            self.logger.critical(f"Exception occurred in _standardize_city() : {e}")
            return pd.DataFrame()

    def dtype_conversion(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert object dtypes to string for better compatibility.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with converted data types
        """
        try:
            self.logger.info("Starting dtype_conversion()")
            
            # Convert object columns to string
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype("string")
                    
            self.logger.info("Successfully converted object types to string")
            return df
            
        except Exception as e:
            self.logger.critical("Exception occurred in dtype_conversion()")
            return pd.DataFrame()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main transformation function that orchestrates all data cleaning steps.
        
        Args:
            df: Raw input DataFrame
            
        Returns:
            Fully transformed and cleaned DataFrame
        """
        try:
            self.logger.info("Starting transformation process...")
            
            if df.empty:
                self.logger.warning("Empty DataFrame received for transformation")
                return df
            
            # Remove duplicates
            df_duplicates = df[df.duplicated(keep=False)]
            if not df_duplicates.empty:
                self.logger.warning(f"Found {len(df_duplicates)} duplicates in dataframe")
                df = df.drop_duplicates()
                self.logger.info("Duplicates removed from dataframe")
            
            # Remove completely null rows
            all_null_rows = df[df.isnull().all(axis=1)]
            if not all_null_rows.empty:
                self.logger.warning(f"Found {len(all_null_rows)} completely null rows")
                df.dropna(how="all")
                self.logger.info("Completely null rows dropped")
            else:
                self.logger.info("No completely null rows found in dataframe")
            
            # Execute all transformation steps
            transformation_steps = [
                self._rename_columns,
                self._normalize_job_titles,
                self._standardize_annual_salaries,
                self._standardize_additional_comp,
                self._standardize_currency_other_cols,
                self._change_time_format,
                self._standardize_income_context,
                self._standardize_country,
                self._standardize_job_context,
                self._standardize_gender,
                self._standardize_race,
                self._standardize_education,
                self._standardize_experience,
                self._standardize_overall_experience,
                self._standardize_state,
                self._standardize_city
            ]
            
            for step in transformation_steps:
                df = step(df)
                if df.empty:
                    self.logger.error(f"Transformation step {step.__name__} returned empty DataFrame")
                    return pd.DataFrame()

            # Final cleaning
            df = df.replace(r'^\s*$', None, regex=True)  # Replace empty strings with None
            df = df.map(lambda x: None if pd.isna(x) else x)  # Replace NaN with None
            self.logger.info("NaN values replaced with None")

            # Data type conversion and column ordering
            df = self.dtype_conversion(df)
            df = df[[
                'Timestamp', 'Age', 'Gender', 'Race', 'Education',
                'Industry', 'Job_Title', 'Seniority', 'Job_Context',
                'Experience_Overall', 'Experience', 'Annual_Salary',
                'Additional_Comp', 'Currency', 'Country', 'State',
                'City', 'Income_Context'
            ]]

            self.logger.info(f"Transformation completed successfully. Final shape: {df.shape}")
            return df
        
        except Exception as e:
            self.logger.critical(f"Exception occurred in transform() : {e}")
            return pd.DataFrame()


def transform_data(df):
    """
    Convenience function to transform data.
    
    Args:
        df (pd.DataFrame): Raw input DataFrame
        
    Returns:
        pd.DataFrame: Transformed DataFrame
    """
    transformer = SalarySurveyTransformer()
    return transformer.transform(df)