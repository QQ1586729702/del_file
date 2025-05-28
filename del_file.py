import re
from logging import getLogger, INFO, StreamHandler, FileHandler, Formatter, Logger
from datetime import datetime
from msvcrt import getch
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


class FileRemover:
    def __init__(self):
        self._all_config = GetConfig.get_all_config()
        self._logger = self.setup_logger()
        self._file_checker = FileChecker(self._all_config, self._logger)
        self._del_file_path = Path(self._all_config[GetConfig.delFilePath])

    @staticmethod
    def setup_logger() -> Logger:
        logger = getLogger('FileRemover')
        logger.setLevel(INFO)
        # 创建一个handler，用于写入日志文件
        fh = FileHandler('file_remover.log', encoding='utf-8')
        fh.setLevel(INFO)
        # 再创建一个handler，用于输出到控制台
        ch = StreamHandler()
        ch.setLevel(INFO)
        # 定义handler的输出格式
        formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # 给logger添加handler
        logger.addHandler(fh)
        logger.addHandler(ch)
        return logger

    def start_remove_file(self):
        files = self._del_file_path.iterdir()
        futures = []
        with ThreadPoolExecutor(max_workers=10) as pool:
            for file in files:
                del_file = Path(file)
                if self._file_checker.check_file(del_file):
                    self._logger.info(
                        f'删除文件: {del_file.name},'
                        f' 创建时间为: {datetime.fromtimestamp(del_file.stat().st_ctime)},'
                        f' 创建星期为: {datetime.fromtimestamp(del_file.stat().st_ctime).isoweekday()},'
                        f' 文件大小为: {del_file.stat().st_size / 1024 / 1024}MB')
                    future = pool.submit(self.remove_file, file=del_file)
                    futures.append(future)
        results = [future.result() for future in as_completed(futures)]
        remove_count = len([result[0] for result in results if result[0]])
        remove_size = sum([result[1] for result in results if result[0]])
        self._logger.info(f'总共删除 {remove_count} 个文件,删除的总大小为: {remove_size / 1024 / 1024}MB')

    def remove_file(self, file: Path) -> (bool, float):
        try:
            file_size = file.stat().st_size
            file.unlink()
            return True, file_size
        except Exception as e:
            self._logger.error(f'删除文件时发生错误:{str(e)}')
            return False, 0


class GetConfig:
    init_config = '''# 删除文件位置
        delFilePath = C://Work//
        # 删除文件类型
        delFileType = txt
        # 开始删除时间
        startDelTime = 2022-08-20
        # 结束删除时间(不包含当天)[开始时间,结束时间)
        endDelTime = 2023-11-24
        # 文件删除名字包含
        fileDeleteNameInclude = *
        # 星期几不删除(创建时间为备份文件的后一天)(1->备份星期天,2->备份星期一)
        retentionWeekOfDay = 6
        # 一个月的哪几天不删除
        retentionMonthOfDay = 01,02'''
    file_path = './config.txt'
    delFilePath = 'delFilePath'
    delFileType = 'delFileType'
    startDelTime = 'startDelTime'
    endDelTime = 'endDelTime'
    fileDeleteNameInclude = 'fileDeleteNameInclude'
    retentionWeekOfDay = 'retentionWeekOfDay'
    retentionMonthOfDay = 'retentionMonthOfDay'

    @classmethod
    def get_config_file(cls) -> str:
        if Path(cls.file_path).exists():
            with open(file=cls.file_path, mode='r', encoding='UTF-8') as file:
                config_file = file.read()
        else:
            with open(file=cls.file_path, mode='w', encoding='UTF-8') as file:
                file.write('\n'.join(line.lstrip() for line in cls.init_config.split('\n')))
                print('创建默认文件成功,请修改配置文件,按下任意键退出')
                getch()
        return config_file

    @classmethod
    def get_all_config(cls) -> dict:
        all_config = cls.get_config_file()
        config_dict = dict(re.findall(r'(\w+)\s*=\s*(.*)', all_config))
        return config_dict


class FileChecker:
    def __init__(self, config, logger):
        self._config = config
        self._logger = logger

    def check_file(self, file: Path):
        if not file.is_file():
            self._logger.warning(f'{file.name} 不是文件')
            return False
        if not self.check_file_name(file):
            self._logger.warning(f'{file.name} 文件名未通过检查'
                                 f'({self._config[GetConfig.fileDeleteNameInclude]}为名称必须包含)')
            return False
        if not self.check_file_type(file):
            self._logger.warning(f'{file.name} 文件类型({file.suffix[1:]})未通过检查'
                                 f'({self._config[GetConfig.delFileType]}为删除类型)')
            return False
        if not self.check_file_time(file):
            self._logger.warning(
                f'{file.name} 文件创建时间({datetime.fromtimestamp(file.stat().st_ctime).strftime("%Y-%m-%d")})未通过检查'
                f'({self._config[GetConfig.startDelTime]}-{self._config[GetConfig.endDelTime]}为删除区间)')
            return False
        if not self.check_file_weekday(file):
            self._logger.warning(f'{file.name} '
                                 f'文件创建星期({datetime.fromtimestamp(file.stat().st_ctime).isoweekday()})未通过检查'
                                 f'({self._config[GetConfig.retentionWeekOfDay]}为每周的保留星期)')
            return False
        if not self.check_file_month_of_day(file):
            self._logger.warning(f'{file.name} '
                                 f'文件创建的月中的日期({datetime.fromtimestamp(file.stat().st_ctime).strftime("%Y-%m-%d")})'
                                 f'未通过检查'
                                 f'({self._config[GetConfig.retentionMonthOfDay]}为每月的保留日期)')
            return False
        return True

    def check_file_name(self, file):
        return file.name.find(self._config[GetConfig.fileDeleteNameInclude]) != -1 or \
               self._config[GetConfig.fileDeleteNameInclude] == '*'

    def check_file_type(self, file):
        return file.suffix[1:] == self._config[GetConfig.delFileType]

    def check_file_time(self, file):
        start_time = datetime.strptime(self._config[GetConfig.startDelTime], '%Y-%m-%d')
        end_time = datetime.strptime(self._config[GetConfig.endDelTime], '%Y-%m-%d')
        creat_time = datetime.fromtimestamp(file.stat().st_ctime)
        return start_time <= creat_time <= end_time

    def check_file_weekday(self, file):
        creat_time = datetime.fromtimestamp(file.stat().st_ctime)
        return str(creat_time.isoweekday()) not in tuple(self._config[GetConfig.retentionWeekOfDay].split(','))

    def check_file_month_of_day(self, file):
        creat_time = datetime.fromtimestamp(file.stat().st_ctime)
        year = creat_time.year
        month = creat_time.strftime('%m')
        file_day = creat_time.strftime('%Y-%m-%d')
        retention_days = []
        for month_of_day in tuple(self._config[GetConfig.retentionMonthOfDay].split(',')):
            retention_days.append(f'{year}-{month}-{month_of_day}')
        return file_day not in retention_days


if __name__ == '__main__':
    fr = FileRemover()
    fr.start_remove_file()
