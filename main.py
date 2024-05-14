from core.scraper import GibddClient
from core.captcha_solver.model import CaptchaSolverModel

from core.data_processor import process_check_response
from excel.xlsx_io import get_vin_list, output_check_result

from commons.commons import Cache
from commons.log import GibddScraperLogger

import asyncio

from datetime import datetime

VIN_LIST_FILE_PATH = 'excel/input/vin_list.xlsx'
OUTPUT_FILE_PATH = f'excel/output/gibdd_{datetime.now().strftime(format="%Y-%m-%d_%H-%M-%S")}.xlsx'


async def main(vin_list: list[str], check_types: list[str]):
	logger = GibddScraperLogger()
	gibdd_client = GibddClient(captcha_solver=CaptchaSolverModel.load(), logger=logger)

	for check_type in check_types:
		vehicles_check_tasks = [gibdd_client.check_vehicle(vin=vin, check_type=check_type) for vin in vin_list]
		for vehicle_check_task in asyncio.as_completed(vehicles_check_tasks):
			try:
				check_response = await vehicle_check_task
				print(check_response)
				check_result, check_type = await asyncio.to_thread(process_check_response, check_response=check_response)
				await asyncio.to_thread(output_check_result, output_excel_file=OUTPUT_FILE_PATH,
				                        check_result=check_result, check_type=check_type)
			except Exception as e:
				logger.error(f'Error: {type(e)} - {e}')


if __name__ == '__main__':
	import time

	# vins = [
	# 	'XWWFT411BA0000039',
	# 	'TMAD281BBDJ015022',
	# 	'XTA219060F0311934',
	# 	'X7LASREA756363961',
	# 	'X9F5XXEED56J14373'
	# ]
	vins = get_vin_list(VIN_LIST_FILE_PATH)
	checks = [
		'Общая информация о ТС',
		'История регистрации',
		'ДТП',
		'Розыск',
		'Ограничения на рег. действия',
		'Диагностическая карта'
	]

	start_time = time.time()
	print('--------------------------- START ---------------------------')

	loop = asyncio.get_event_loop()
	loop.run_until_complete(main(vins, checks))

	print('--------------------------- FINISH ---------------------------')
	print(f'Time: {time.time() - start_time}')
