import pandas as pd

from .entsoerawclient import EntsoeRawClient
from .mappings import BIDDING_ZONES
from .mappings import TIMEZONE_MAPPINGS
from .misc import day_limited
from .misc import paginated
from .misc import year_limited
from .parsers import parse_crossborder_flows
from .parsers import parse_generation
from .parsers import parse_imbalance_prices
from .parsers import parse_loads
from .parsers import parse_prices
from .parsers import parse_unavailabilities
from .parsers import parse_units


class EntsoePandasClient(EntsoeRawClient):
    @year_limited
    def query_day_ahead_prices(self, country_code, start, end) -> pd.Series:
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        text = super(EntsoePandasClient, self).query_day_ahead_prices(
            country_code=country_code, start=start, end=end)
        series = parse_prices(text)
        series = series.tz_convert(TIMEZONE_MAPPINGS[country_code])
        return series

    @year_limited
    def query_load(self, country_code, start, end) -> pd.Series:
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        text = super(EntsoePandasClient, self).query_load(
            country_code=country_code, start=start, end=end)
        series = parse_loads(text)
        series = series.tz_convert(TIMEZONE_MAPPINGS[country_code])
        return series

    @year_limited
    def query_generation_forecast(self, country_code, start, end, psr_type=None,
                                  lookup_bzones=False):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type
        lookup_bzones : bool
            if True, country_code is expected to be a bidding zone

        Returns
        -------
        pd.DataFrame
        """
        text = super(EntsoePandasClient, self).query_generation_forecast(
            country_code=country_code, start=start, end=end, psr_type=psr_type,
            lookup_bzones=lookup_bzones)
        df = parse_generation(text)
        df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
        return df

    @year_limited
    def query_generation(self, country_code, start, end, psr_type=None,
                         lookup_bzones=False):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter on a single psr type
        lookup_bzones : bool
            if True, country_code is expected to be a bidding zone

        Returns
        -------
        pd.DataFrame
        """
        text = super(EntsoePandasClient, self).query_generation(
            country_code=country_code, start=start, end=end, psr_type=psr_type,
            lookup_bzones=lookup_bzones)
        df = parse_generation(text)
        df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
        return df

    @year_limited
    def query_installed_generation_capacity(self, country_code, start, end,
                                            psr_type=None):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        pd.DataFrame
        """
        text = super(
            EntsoePandasClient, self).query_installed_generation_capacity(
            country_code=country_code, start=start, end=end, psr_type=psr_type)
        df = parse_generation(text)
        df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
        return df

    @year_limited
    def query_crossborder_flows(self, country_code_from, country_code_to, start, end):
        """
        Note: Result will be in the timezone of the origin country

        Parameters
        ----------
        country_code_from : str
        country_code_to : str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.Series
        """
        text = super(EntsoePandasClient, self).query_crossborder_flows(
            country_code_from=country_code_from,
            country_code_to=country_code_to, start=start, end=end)
        ts = parse_crossborder_flows(text)
        ts = ts.tz_convert(TIMEZONE_MAPPINGS[country_code_from])
        return ts

    @year_limited
    def query_imbalance_prices(self, country_code, start, end, psr_type=None):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        psr_type : str
            filter query for a specific psr type

        Returns
        -------
        pd.DataFrame
        """
        text = super(EntsoePandasClient, self).query_imbalance_prices(
            country_code=country_code, start=start, end=end, psr_type=psr_type)
        df = parse_imbalance_prices(text)
        df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
        return df

    @year_limited
    @paginated
    def query_unavailability_of_generation_units(self, country_code, start, end,
                                                 docstatus=None):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        docstatus : str, optional

        Returns
        -------
        pd.DataFrame
        """
        content = super(EntsoePandasClient,
                        self).query_unavailability_of_generation_units(
            country_code=country_code, start=start, end=end,
            docstatus=docstatus)
        df = parse_unavailabilities(content)
        df = df.tz_convert(TIMEZONE_MAPPINGS[country_code])
        df['start'] = df['start'].apply(
            lambda x: x.tz_convert(TIMEZONE_MAPPINGS[country_code]))
        df['end'] = df['end'].apply(
            lambda x: x.tz_convert(TIMEZONE_MAPPINGS[country_code]))
        return df

    def query_withdrawn_unavailability_of_generation_units(
            self, country_code, start, end):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        pd.DataFrame
        """
        df = self.query_unavailability_of_generation_units(
            country_code=country_code, start=start, end=end, docstatus='A13')
        return df

    @day_limited
    def query_units(self, bz_domain, start, end, psr_type=None):
        """
        """
        content = super(EntsoePandasClient, self).query_units(
            country_code=BIDDING_ZONES[bz_domain],
            start=start, end=end, psr_type=psr_type)
        df = parse_units(content).tz_convert(TIMEZONE_MAPPINGS[bz_domain])
        df['start'] = df['start'].apply(
            lambda x: x.tz_convert(TIMEZONE_MAPPINGS[bz_domain]))
        df['end'] = df['end'].apply(
            lambda x: x.tz_convert(TIMEZONE_MAPPINGS[bz_domain]))
        return df
