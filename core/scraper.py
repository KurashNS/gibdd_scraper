import ua_generator
from aiohttp_socks import ProxyConnector
from aiohttp import ClientSession, ClientError, ClientResponseError

from core.captcha_solver.model import CaptchaSolverModel

from tenacity import retry, retry_if_exception_type, wait_random, stop_after_attempt

from aiohttp_socks import ProxyConnectionError, ProxyError, ProxyTimeoutError
from asyncio.exceptions import TimeoutError as AioTimeoutError

import asyncio

from logging import Logger, INFO, StreamHandler

from commons.commons import Cache

check_vehicle_retry_exceptions = (ProxyConnectionError, ProxyError, ProxyTimeoutError,
                                  ClientError, AioTimeoutError, ValueError)


class GibddClient:
	_proxy_server_url = 'http://yfy5n4:s4SsUv@185.82.126.71:13518'

	def __init__(self, captcha_solver: CaptchaSolverModel, logger: Logger) -> None:
		self._ua = ua_generator.generate(device='desktop')
		self._headers = {
			"Host": "check.gibdd.ru",
			"Origin": "https://xn--90adear.xn--p1ai",
			"Referer": "https://xn--90adear.xn--p1ai/",
			"Sec-Ch-Ua": self._ua.ch.brands,
			"Sec-Ch-Ua-Mobile": self._ua.ch.mobile,
			"Sec-Ch-Ua-Platform": self._ua.ch.platform,
			"Sec-Fetch-Dest": "empty",
			"Sec-Fetch-Mode": "cors",
			"Sec-Fetch-Site": "cross-site",
			"User-Agent": self._ua.text
		}

		self._captcha_solver = captcha_solver

		self._logger = logger
		self._cache = Cache()

		self._semaphore = asyncio.Semaphore(value=50)

	@retry(sleep=asyncio.sleep, retry=retry_if_exception_type(AioTimeoutError),
	       stop=stop_after_attempt(10), reraise=True)
	async def _get_captcha(self, session: ClientSession) -> dict[str: str]:
		async with session.get(url='https://check.gibdd.ru/captcha', timeout=10) as captcha_response:
			captcha = await captcha_response.json()

		captcha_b64img = captcha['base64jpg']
		captcha_word = await asyncio.to_thread(self._captcha_solver.solve_captcha, captcha_b64img=captcha_b64img)

		captcha.update({'word': captcha_word})
		return captcha

	@staticmethod
	async def _prepare_vehicle_check_request(captcha: dict[str: str], vin: str, check_type: str) -> tuple[str, dict]:
		check_types_mapping = {
			('Общая информация о ТС', 'История регистрации'): ('history', 'register'),
			'ДТП': ('auisdtp', 'dtp'),
			'Розыск': ('wanted', 'wanted'),
			'Ограничения на рег. действия': ('restricted', 'restrict'),
			'Диагностическая карта': ('diagnostic', 'diagnostic'),
		}
		for check_type_name, (check_type_codename, vehicle_check_endpoint) in check_types_mapping.items():
			if check_type in check_type_name:
				break
		else:
			raise TypeError(f'Incorrect check type - {check_type}')

		vehicle_check_url = 'https://xn--b1afk4ade.xn--90adear.xn--p1ai/proxy/check/auto/' + vehicle_check_endpoint
		vehicle_check_data = {
			'vin': vin,
			'checkType': check_type_codename,
			'captchaWord': captcha['word'],
			'captchaToken': captcha['token'],
		}

		return vehicle_check_url, vehicle_check_data

	@retry(retry=retry_if_exception_type(check_vehicle_retry_exceptions), sleep=asyncio.sleep,
	       wait=wait_random(min=3, max=5), stop=stop_after_attempt(5), reraise=True)
	async def _make_vehicle_check_request(self, vin: str, check_type: str):
		async with ProxyConnector.from_url(url=self._proxy_server_url) as proxy_conn:
			async with ClientSession(connector=proxy_conn, headers=self._headers, raise_for_status=True) as session:
				captcha = await self._get_captcha(session=session)
				vehicle_check_url, vehicle_check_data = await self._prepare_vehicle_check_request(
					vin=vin,
					check_type=check_type,
					captcha=captcha
				)

				async with session.post(url=vehicle_check_url, data=vehicle_check_data) as vehicle_check_response:
					check_response = await vehicle_check_response.json()
					self._logger.info(f'VIN: {vin} - Check type: {check_type} | Vehicle check response: {check_response}')
					return check_response

	@retry(retry=retry_if_exception_type(check_vehicle_retry_exceptions),
	       sleep=asyncio.sleep, stop=stop_after_attempt(10), reraise=True)
	async def check_vehicle(self, vin: str, check_type: str) -> dict:
		async with self._semaphore:
			if check_type in ('Общая информация о ТС', 'История регистрации'):
				check_response = self._cache.get(key=vin)
				if check_response:
					self._cache.delete(key=vin)
					check_response['checkType'] = check_type
					return check_response

			check_response = await self._make_vehicle_check_request(vin=vin, check_type=check_type)

			if check_response.get('status', 0) == 200:
				check_response['checkType'] = check_type
				if check_type in ('Общая информация о ТС', 'История регистрации'):
					self._cache.set(key=vin, value=check_response)
				return check_response
			elif check_response.get('code', 0) == 201:
				self._logger.info(f'VIN: {vin} - Check type: {check_type} | No CAPTCHA validation')
				raise ValueError('No CAPTCHA validation')
			elif check_response.get('status', 0) == 404 or check_response.get('code', 0) == 404:
				self._logger.info(f'VIN: {vin} - Check type: {check_type} | No record')
				raise TypeError('No record')
			else:
				self._logger.info(f'VIN: {vin} - Check type: {check_type} | Bad vehicle check response')
				raise ValueError('Bad vehicle check response')


if __name__ == '__main__':
	async def main():
		logger = Logger(name='root', level=INFO)
		logger.addHandler(hdlr=StreamHandler())

		gibdd_client = GibddClient(captcha_solver=CaptchaSolverModel.load(), logger=logger)

		vin_list = [
	           'XWEL2416BL0000433',
	           'X9FKXXEEBKBM40204',
	           'XWEGU411BK0013037',
	           'Z94C241BBNR254203',
	           'XW8ZZZ61ZJG051127'
	       ] * 100000
		test_tasks = [gibdd_client.check_vehicle(vin=vin, check_type='ДТП') for vin in vin_list]
		for test_task in asyncio.as_completed(test_tasks):
			try:
				await test_task
			except Exception as e:
				print(f'Error: {type(e)} - {e}')
				logger.error(f'Error: {type(e)} - {e}')


	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())
