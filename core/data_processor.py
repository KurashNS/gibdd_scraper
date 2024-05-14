from __future__ import annotations
from typing import Optional

import pandas as pd


def json_to_df(
		data: dict | list[dict],
		col_mapping: Optional[dict[str: str]] = None,
		values_mapping: Optional[dict[str: dict[str: str]]] = None,
		col_types: Optional[dict[str: str]] = None,
		insert_status_column: bool = True,
		status: Optional[str] = None,
		insert_vin_column: bool = False,
		vin: Optional[str] = None,
) -> pd.DataFrame:
	try:
		df = pd.json_normalize(data=data)

		if col_mapping:
			df = df.rename(columns=col_mapping)[col_mapping.values()]

		if values_mapping:
			for value, mapping in values_mapping.items():
				df[value] = df[value].map(mapping)

		if col_types:
			for col, dtype in col_types.items():
				if dtype == 'ru_datetime':
					df[col] = pd.to_datetime(df[col], format='%d.%m.%Y %H:%M').dt.date
				elif dtype == 'ru_date':
					df[col] = pd.to_datetime(df[col], format='%d.%m.%Y').dt.date
				elif dtype == 'ru_date_str':
					df[col] = pd.to_datetime(df[col].astype('datetime64[ns]'), format='%d.%m.%Y').dt.date
				else:
					df[col] = df[col].astype(dtype)

		if insert_vin_column:
			df.insert(loc=0, column='VIN-номер', value=vin)
		if insert_status_column:
			if not status:
				status = 'Успешно'
			df.insert(loc=1, column='Статус', value=status)

		return df
	except Exception as e:
		print(f'Ошибка: json_to_df - {e}')
		if not vin:
			vin = data['VIN-номер']

		return pd.DataFrame.from_dict({'VIN-номер': vin, 'Статус': 'Ошибка при обработке результата'})


def process_vehicle_general_info(vehicle_general_info_check_response: dict) -> pd.DataFrame:
	try:
		vehicle_info = vehicle_general_info_check_response['RequestResult']

		vehicle_id = vehicle_info['id']
		reestr_status = vehicle_info['reestr_status']

		vin = vehicle_info['vehicle_vin']
		register_history = vehicle_info['periods']
	except (TypeError, KeyError) as e:
		raise IncorrectResponse(
			f'Incorrect vehicle general info check response ({type(e)} - {e}): {vehicle_general_info_check_response}'
		)

	if register_history and vehicle_id and reestr_status:
		vehicle_info_processing_args = {
			'data': vehicle_info,
			'col_mapping': {
				'vehicle_vin': 'VIN-номер',
				'vehicle_brandmodel': 'Марка, модель',
				'vehicle_releaseyear': 'Год выпуска',
				'vehicle_bodycolor': 'Цвет',
				'vehicle_type_name': 'Тип ТС',
				'vehicle_enclosedvolume': 'Объём двигателя, куб. см',
				'vehicle_enginepowerkw': 'Мощность двигателя, КВт',
				'vehicle_enginepower': 'Мощность двигателя, ЛС',
				'vehicle_eco_class': 'Экологический класс',
				'reestr_status': 'Тип записи'
			},
			'vin': vin
		}
	else:
		vehicle_info_processing_args = {
			'data': {
				'VIN-номер': vin,
				'Марка, модель': '',
				'Год выпуска': '',
				'Цвет': '',
				'Тип ТС': '',
				'Объём двигателя, куб. см': '',
				'Мощность двигателя, КВт': '',
				'Мощность двигателя, ЛС': '',
				'Экологический класс': '',
				'Тип записи': ''
			},
			'status': 'ТС с таким VIN не зарегистрировано в ГИБДД'
		}

	return json_to_df(**vehicle_info_processing_args)


def process_register_history_data(register_history_check_response: dict) -> pd.DataFrame:
	try:
		vehicle_info = register_history_check_response['RequestResult']

		vehicle_id = vehicle_info['id']
		reestr_status = vehicle_info['reestr_status']

		vin = vehicle_info['vehicle_vin']
		register_history = vehicle_info['periods']
	except (TypeError, KeyError) as e:
		raise IncorrectResponse(
			f'Incorrect vehicle general info check response ({type(e)} - {e}): {vehicle_general_info_check_response}'
		)

	if register_history and vehicle_id and reestr_status:
		register_history_processing_args = {
			'data': register_history,
			'col_mapping': {
				'ownerType': 'Владелец',
				'startDate': 'Начало владения',
				'endDate': 'Окончание владения'
			},
			'col_types': {
				'Начало владения': 'ru_date',
				'Окончание владения': 'ru_date'
			},
			'insert_vin_column': True,
			'vin': vin
		}
	else:
		register_history_processing_args = {
			'data': {
				'VIN-номер': vin,
				'Владелец': '',
				'Начало владения': '',
				'Окончание владения': ''
			},
			'status': 'ТС с таким VIN не зарегистрировано в ГИБДД'
		}

	return json_to_df(**register_history_processing_args)


def process_traffic_accidents_data(traffic_accidents_check_response: dict) -> pd.DataFrame:
	try:
		vin = traffic_accidents_check_response['vin']
		accidents = traffic_accidents_check_response['RequestResult']['Accidents']

		status_code = traffic_accidents_check_response['RequestResult']['statusCode']
		request_error_desc = traffic_accidents_check_response['RequestResult']['errorDescription']
	except (TypeError, KeyError) as e:
		raise IncorrectResponse(
			f'Incorrect traffic accidents check response ({type(e)} - {e}): {traffic_accidents_check_response}'
		)

	if accidents:
		accidents_data_processing_args = {
			'data': accidents,
			'col_mapping': {
				'VehicleMark': 'Марка',
				'VehicleModel': 'Модель',
				'VehicleYear': 'Год выпуска',
				'AccidentDateTime': 'Дата, время происшествия',
				'AccidentPlace': 'Место происшествия',
				'AccidentType': 'Тип происшествия',
				'VehicleDamageState': 'Состояние автомобиля',
				'VehicleSort': 'Номер ТС в ДТП',
				'VehicleAmount': 'Всего ТС в ДТП',
				'OwnerOkopf': 'ОПФ собственника',
				'AccidentNumber': 'Номер происшествия'
			},
			'col_types': {
				'Дата, время происшествия': 'ru_datetime'
			},
			'insert_vin_column': True,
			'vin': vin
		}
	else:
		if status_code == 1:
			error_desc = 'ТС не попадало в ДТП'
		else:
			if request_error_desc:
				error_desc = request_error_desc
			else:
				error_desc = 'Ошибка сервера ГИБДД'

		accidents_data_processing_args = {
			'data': {
				'VIN-номер': vin,
				'Марка': '',
				'Модель': '',
				'Год выпуска': '',
				'Дата, время происшествия': '',
				'Место происшествия': '',
				'Тип происшествия': '',
				'Состояние автомобиля': '',
				'Номер ТС в ДТП': '',
				'Всего ТС в ДТП': '',
				'ОПФ собственника': '',
				'Номер происшествия': '',
			},
			'status': error_desc
		}

	return json_to_df(**accidents_data_processing_args)


def process_wanted_data(wanted_check_response: dict) -> pd.DataFrame:
	try:
		vin = wanted_check_response['vin']
		wanted_records = wanted_check_response['RequestResult']['records']

		status_code = wanted_check_response['RequestResult']['error']
	except (TypeError, KeyError) as e:
		raise IncorrectResponse(f'Incorrect wanted check response ({type(e)} - {e}): {wanted_check_response}')

	if wanted_records:
		wanted_data_processing_args = {
			'data': wanted_records,
			'col_mapping': {
				'w_vin': 'VIN-номер',
				'w_rec': 'Номер записи',
				'w_model': 'Марка, модель ТС',
				'w_god_vyp': 'Год выпуска ТС',
				'w_reg_zn': 'Гос. рег. номер',
				'w_kuzov': 'Номер кузова',
				'w_shassi': 'Номер шасси',
				'w_dvig': 'Номер двигателя',
				'w_data_pu': 'Дата постоянного учета в розыске',
				'w_reg_inic': 'Регион инициатора розыска'
			},
			'col_types': {
				'Дата постоянного учета в розыске': 'ru_date'
			},
		}
	else:
		if status_code == 0:
			error_desc = 'ТС не в розыске'
		else:
			error_desc = 'Ошибка сервера ГИБДД'

		wanted_data_processing_args = {
			'data': {
				'VIN-номер': vin,
				'Номер записи': '',
				'Марка, модель ТС': '',
				'Год выпуска ТС': '',
				'Гос. рег. номер': '',
				'Номер кузова': '',
				'Номер шасси': '',
				'Номер двигателя': '',
				'Дата постоянного учета в розыске': '',
				'Регион инициатора розыска': ''
			},
			'status': error_desc
		}

	return json_to_df(**wanted_data_processing_args)


def process_restrictions_data(restrictions_check_response: dict) -> pd.DataFrame:
	try:
		vin = restrictions_check_response['vin']
		restrictions_records = restrictions_check_response['RequestResult']['records']

		status_code = restrictions_check_response['RequestResult']['error']
	except (TypeError, KeyError) as e:
		raise IncorrectResponse(f'Incorrect wanted check response ({type(e)} - {e}): {restrictions_check_response}')

	if restrictions_records:
		restrictions_recs_processing_args = {
			'data': restrictions_records,
			'col_mapping': {
				'tsVIN': 'VIN-номер',
				'tsmodel': 'Марка, модель ТС',
				'tsyear': 'Год выпуска ТС',
				'dateadd': 'Дата наложения ограничения',
				'regname': 'Регион инициатора ограничения',
				'divtype': 'Кем наложено ограничение',
				'ogrkod': 'Вид ограничения',
				'osnOgr': 'Основание',
				'phone': 'Телефон инициатора',
				'gid': 'Ключ ГИБДД'
			},
			'values_mapping': {
				'Вид ограничения': {
					'0': '',
					'1': 'Запрет на регистрационные действия',
					'2': 'Запрет на снятие с учета',
					'3': 'Запрет на регистрационные действия и прохождение ГТО',
					'4': 'Утилизация (для транспорта не старше 5 лет)',
					'5': 'Аннулирование'
				},
				'Кем наложено ограничение': {
					'0': 'не предусмотренный код',
					'1': 'Судебные органы',
					'2': 'Судебный пристав',
					'3': 'Таможенные органы',
					'4': 'Органы социальной защиты',
					'5': 'Нотариус',
					'6': 'ОВД или иные правоохр. органы',
					'7': 'ОВД или иные правоохр. органы (прочие)'
				}
			},
			'col_types': {
				'Дата наложения ограничения': 'ru_date'
			},
		}
	else:
		if status_code == 0:
			error_desc = 'Нет ограничений на рег. действия'
		else:
			error_desc = 'Ошибка сервера ГИБДД'

		restrictions_recs_processing_args = {
			'data': {
				'VIN-номер': vin,
				'Марка, модель ТС': '',
				'Год выпуска ТС': '',
				'Дата наложения ограничения': '',
				'Регион инициатора ограничения': '',
				'Кем наложено ограничение': '',
				'Вид ограничения': '',
				'Основание': '',
				'Телефон инициатора': '',
				'Ключ ГИБДД': ''
			},
			'status': error_desc
		}

	return json_to_df(**restrictions_recs_processing_args)


def process_diagnostic_card_data(diagnostic_card_check_response: dict) -> pd.DataFrame:
	try:
		vin = diagnostic_card_check_response['vin']
		diagnostic_cards = diagnostic_card_check_response['RequestResult']['diagnosticCards']

		request_error = diagnostic_card_check_response['RequestResult']['error']
		request_error_desc = diagnostic_card_check_response['RequestResult']['status']
	except (TypeError, KeyError) as e:
		raise IncorrectResponse(
			f'Incorrect diagnostic card response ({type(e)} - {e}): {diagnostic_card_check_response}'
		)

	if diagnostic_cards:
		all_dcs_list = []
		for diagnostic_card in diagnostic_cards:
			current_dc_df = json_to_df(
				data=diagnostic_card,
				col_mapping={
					'vin': 'VIN-номер',
					'dcNumber': 'Номер ДК',
					'dcDate': 'Дата ДК',
					'odometerValue': 'Показания одометра',
					'body': 'Номер кузова',
					'brand': 'Марка',
					'model': 'Модель',
					'chassis': 'Шасси',
					'dcExpirationDate': 'Срок ДК',
					'pointAddress': 'Адрес проведения ТО'
				},
				col_types={
					'Дата ДК': 'ru_date_str',
					'Срок ДК': 'ru_date_str',
				}
			)

			previous_dcs = diagnostic_card['previousDcs']
			if previous_dcs:
				previous_dcs_df = json_to_df(
					data=previous_dcs,
					col_mapping={
						'dcDate': 'Дата ДК',
						'dcExpirationDate': 'Срок ДК',
						'odometerValue': 'Показания одометра'
					},
					col_types={
						'Дата ДК': 'ru_date_str',
						'Срок ДК': 'ru_date_str',
					},
					insert_vin_column=True,
					vin=diagnostic_card['vin']
				)

				diagnostic_card_df = pd.concat([current_dc_df, previous_dcs_df])
				all_dcs_list.append(diagnostic_card_df)
			else:
				all_dcs_list.append(current_dc_df)

		all_dcs_df = pd.concat(all_dcs_list)
		return all_dcs_df
	else:
		if request_error:
			error_desc = request_error_desc
		else:
			error_desc = 'Нет данных о ДК для данного ТС'

		error_data = {
			'VIN-номер': vin,
			'Номер ДК': '',
			'Дата ДК': '',
			'Показания одометра': '',
			'Номер кузова': '',
			'Марка': '',
			'Модель': '',
			'Шасси': '',
			'Срок ДК': '',
			'Адрес проведения ТО': ''
		}
		return json_to_df(data=error_data, status=error_desc)


def process_check_response(check_response: Optional[dict]) -> tuple[pd.DataFrame, str]:
	try:
		check_type_func_mapping = {
			'Общая информация о ТС': process_vehicle_general_info,
			'История регистрации': process_register_history_data,
			'ДТП': process_traffic_accidents_data,
			'Розыск': process_wanted_data,
			'Ограничения на рег. действия': process_restrictions_data,
			'Диагностическая карта': process_diagnostic_card_data
		}
		if check_response:
			check_type = check_response['checkType']
			check_result = check_type_func_mapping[check_type](check_response)
			return check_result, check_type
	except Exception as e:
		raise Exception(f'Unable to process check response ({type(e)} - {e}): {check_response}')


if __name__ == '__main__':
	pass
