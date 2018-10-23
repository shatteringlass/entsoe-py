from .misc import retry

import pytz
import requests

from bs4 import BeautifulSoup

from .exceptions import NoMatchingDataError
from .exceptions import PaginationError
from .mappings import BIDDING_ZONES
from .mappings import DOCUMENTTYPE
from .mappings import DOMAIN_MAPPINGS

URL = 'https://transparency.entsoe.eu/api'


class EntsoeRawClient:
    """
    Client to perform API calls and return the raw responses
    API-documentation available at webpage:
    https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_request_methods

    Attributions: Parts of the code for parsing Entsoe responses were copied
    from https://github.com/tmrowco/electricitymap
    """

    def __init__(self, api_key, session=None, retry_count=1, retry_delay=0,
                 proxies=None):
        """
        Parameters
        ----------
        api_key : str
        session : requests.Session
        retry_count : int
            number of times to retry the call if the connection fails
        retry_delay: int
            amount of seconds to wait between retries
        proxies : dict
            requests proxies
        """
        if api_key is None:
            raise TypeError("API key cannot be None")
        self.api_key = api_key
        if session is None:
            session = requests.Session()
        self.session = session
        self.proxies = proxies
        self.retry_count = retry_count
        self.retry_delay = retry_delay

    @retry
    def base_request(self, params, start, end):
        """
        Parameters
        ----------
        params : dict
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        requests.Response
        """
        start_str = self._datetime_to_str(start)
        end_str = self._datetime_to_str(end)

        base_params = {
            'securityToken': self.api_key,
            'periodStart': start_str,
            'periodEnd': end_str
        }
        params.update(base_params)

        response = self.session.get(url=URL, params=params,
                                    proxies=self.proxies)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            soup = BeautifulSoup(response.text, 'html.parser')
            text = soup.find_all('text')
            if len(text):
                error_text = soup.find('text').text
                if 'No matching data found' in error_text:
                    raise NoMatchingDataError
                elif 'amount of requested data \
                      exceeds allowed limit' in error_text:
                    requested = error_text.split(' ')[-2]
                    raise PaginationError(
                        f"The API is limited to 200 elements per request. \
                          This query requested for {requested} documents and \
                          cannot be fulfilled as is.")
            raise e
        else:
            return response

    @staticmethod
    def _endpoint_to_doctype(endpoint: str):
        for (k, v) in DOCUMENTTYPE.items():
            if v == endpoint:
                return k
        return None

    @staticmethod
    def _datetime_to_str(dtm):
        """
        Convert a datetime object to a string in UTC
        of the form YYYYMMDDhh00

        Parameters
        ----------
        dtm : pd.Timestamp
            Recommended to use a timezone-aware object!
            If timezone-naive, UTC is assumed

        Returns
        -------
        str
        """
        if dtm.tzinfo is not None and dtm.tzinfo != pytz.UTC:
            dtm = dtm.tz_convert("UTC")
        fmt = '%Y%m%d%H00'
        ret_str = dtm.strftime(fmt)
        return ret_str

    def query_day_ahead_prices(self, country_code, start, end):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        str
        """
        domain = BIDDING_ZONES[country_code]
        params = {
            'documentType': self._endpoint_to_doctype('Price Document'),
            'in_Domain': domain,
            'out_Domain': domain
        }
        response = self.base_request(params=params, start=start, end=end)
        return response.text

    def query_load(self, country_code, start, end):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        str
        """
        domain = BIDDING_ZONES[country_code]
        params = {
            'documentType': self._endpoint_to_doctype('System total load'),
            'processType': 'A16',
            'outBiddingZone_Domain': domain,
            'out_Domain': domain
        }
        response = self.base_request(params=params, start=start, end=end)
        return response.text

    def query_generation_forecast(self, country_code, start, end, psr_type=None, lookup_bzones=False):
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
        str
        """
        if not lookup_bzones:
            domain = DOMAIN_MAPPINGS[country_code]
        else:
            domain = BIDDING_ZONES[country_code]

        params = {
            'documentType': self._endpoint_to_doctype('Wind and solar forecast'),
            'processType': 'A01',
            'in_Domain': domain,
        }
        if psr_type:
            params.update({'psrType': psr_type})

        response = self.base_request(params=params, start=start, end=end)
        return response.text

    def query_generation(self, country_code, start, end, psr_type=None, lookup_bzones=False):
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
        str
        """
        if not lookup_bzones:
            domain = DOMAIN_MAPPINGS[country_code]
        else:
            domain = BIDDING_ZONES[country_code]

        params = {
            'documentType': self._endpoint_to_doctype('Actual generation per type'),
            'processType': 'A16',
            'in_Domain': domain,
        }
        if psr_type:
            params.update({'psrType': psr_type})

        response = self.base_request(params=params, start=start, end=end)
        return response.text

    def query_installed_generation_capacity(self, country_code, start, end, psr_type=None):
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
        str
        """
        domain = DOMAIN_MAPPINGS[country_code]
        params = {
            'documentType': self._endpoint_to_doctype('Installed generation per type'),
            'processType': 'A33',
            'in_Domain': domain,
        }
        if psr_type:
            params.update({'psrType': psr_type})

        response = self.base_request(params=params, start=start, end=end)
        return response.text

    def query_crossborder_flows(self, country_code_from, country_code_to, start, end):
        """
        Parameters
        ----------
        country_code_from : str
        country_code_to : str
        start : pd.Timestamp
        end : pd.Timestamp

        Returns
        -------
        str
        """
        domain_in = DOMAIN_MAPPINGS[country_code_to]
        domain_out = DOMAIN_MAPPINGS[country_code_from]
        params = {
            'documentType': self._endpoint_to_doctype('Aggregated energy data report'),
            'in_Domain': domain_in,
            'out_Domain': domain_out
        }
        response = self.base_request(params=params, start=start, end=end)
        return response.text

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
        str
        """
        domain = DOMAIN_MAPPINGS[country_code]
        params = {
            'documentType': self._endpoint_to_doctype('Imbalance prices'),
            'controlArea_Domain': domain,
        }
        if psr_type:
            params.update({'psrType': psr_type})
        response = self.base_request(params=params, start=start, end=end)
        return response.text

    def query_unavailability_of_generation_units(self,
                                                 country_code, start, end,
                                                 docstatus=None) -> bytes:
        """
        This endpoint serves ZIP files.
        The query is limited to 200 items per request.

        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        docstatus : str, optional

        Returns
        -------
        bytes
        """
        domain = DOMAIN_MAPPINGS[country_code]
        params = {
            'documentType': self._endpoint_to_doctype('Production unavailability'),
            'biddingZone_domain': domain
            # ,'businessType': 'A53 (unplanned) | A54 (planned)'
        }

        if docstatus:
            params['docStatus'] = docstatus

        response = self.base_request(params=params, start=start, end=end)

        return response.content

    def query_withdrawn_unavailability_of_generation_units(
            self, country_code, start, end):
        """
        Parameters
        ----------
        country_code : str
        start : pd.Timestamp
        end : pd.Timestamp
        """
        content = self.query_unavailability_of_generation_units(
            country_code=country_code, start=start, end=end, docstatus='A13')
        return content

    def query_units(self, bz_domain, impementation_dt, start, end, psr_type=None):
        """
        """
        domain = BIDDING_ZONES[bz_domain]
        params = {
            'documentType': self._endpoint_to_doctype('Configuration document'),
            'biddingZone_domain': domain,
            'businessType': 'B11'
        }

        if psr_type:
            params['psrType'] = psr_type

        response = self.base_request(params=params, start=start, end=end)

        return response.content
