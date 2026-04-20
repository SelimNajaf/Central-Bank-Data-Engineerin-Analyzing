import os
import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataLoadError(Exception):
    """Məlumatların yüklənməsi zamanı baş verən xətalar üçün xüsusi istisna (exception) sinfi."""
    pass


class CsvToSqlPipeline:
    """
    CSV fayllarını oxumaq, təmizləmək və SQL verilənlər bazasına yükləmək üçün pipeline.

    Attributes:
        db_uri (str): Verilənlər bazasına qoşulmaq üçün URI.
        data_dir (Path): CSV fayllarının yerləşdiyi qovluğun yolu.
        engine (Engine): SQLAlchemy mühərrik (engine) obyekti.
    """

    def __init__(self, db_uri: str, data_dir: Path) -> None:
        """
        CsvToSqlPipeline sinfini inisializasiya edir.

        Args:
            db_uri (str): Verilənlər bazasına qoşulmaq üçün bağlantı sətri (connection string).
            data_dir (Path): Verilənlərin saxlanıldığı qovluq yolu.
            
        Raises:
            ValueError: Əgər db_uri və ya data_dir təmin edilməyibsə.
        """
        if not db_uri:
            logger.error("Database URI təqdim edilməyib.")
            raise ValueError("Database URI (DB_URI) mütləq təyin edilməlidir.")
        
        if not data_dir.exists() or not data_dir.is_dir():
            logger.error(f"Verilənlər qovluğu tapılmadı: {data_dir}")
            raise ValueError(f"Təqdim edilmiş qovluq mövcud deyil: {data_dir}")

        self.data_dir = data_dir
        
        try:
            self.engine: Engine = create_engine(db_uri)
            logger.info("Database mühərriki (engine) uğurla yaradıldı.")
        except SQLAlchemyError as e:
            logger.error(f"Database mühərriki yaradılarkən xəta baş verdi: {e}")
            raise DataLoadError(f"Database bağlantısı qurula bilmədi: {e}") from e

    def _clean_dataframe(self, df: pd.DataFrame, exclude_cols: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Dataframe-in sütun adlarını standartlaşdırır və məlumat tiplərini təmizləyir.

        Sütun adlarındakı boşluqları '_' ilə əvəz edir və kiçik hərflərə çevirir.
        İstisna edilmiş sütunlar xaricindəki bütün sütunları rəqəmsal formata (numeric) salır.

        Args:
            df (pd.DataFrame): Təmizlənəcək Pandas Dataframe.
            exclude_cols (Optional[List[str]]): Təmizləmə əməliyyatından istisna ediləcək sütun adları.

        Returns:
            pd.DataFrame: Təmizlənmiş Dataframe.
            
        Raises:
            DataLoadError: Təmizləmə zamanı gözlənilməz xəta olarsa.
        """
        if exclude_cols is None:
            exclude_cols = ['date']

        try:
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

            cols_to_process = [col for col in df.columns if col not in exclude_cols]
            for col in cols_to_process:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace('.', '', regex=False)
                    .str.replace(',', '.', regex=False)
                )
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
            return df
        except Exception as e:
            logger.error(f"Dataframe təmizlənərkən xəta baş verdi: {e}")
            raise DataLoadError(f"Təmizləmə prosesi uğursuz oldu: {e}") from e

    def _load_to_db(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Dataframe-i verilənlər bazasına yükləyir.

        Args:
            df (pd.DataFrame): Yüklənəcək məlumatlar.
            table_name (str): Verilənlər bazasındakı cədvəlin adı.
            
        Raises:
            DataLoadError: Məlumatlar DB-yə yazılarkən xəta yaranarsa.
        """
        try:
            logger.info(f"'{table_name}' cədvəlinə məlumatlar yüklənir...")
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists="replace",
                index=True
            )
            logger.info(f"'{table_name}' cədvəlinə yükləmə uğurla tamamlandı.")
        except SQLAlchemyError as e:
            logger.error(f"'{table_name}' cədvəlinə yazılarkən DB xətası baş verdi: {e}")
            raise DataLoadError(f"DB yükləmə xətası: {e}") from e
        except Exception as e:
            logger.error(f"Gözlənilməz xəta baş verdi: {e}")
            raise DataLoadError(f"Yükləmə uğursuz oldu: {e}") from e

    def run(self) -> None:
        """
        Qovluqdakı bütün CSV fayllarını tapır, təmizləyir və DB-yə yükləyir.
        """
        logger.info(f"Proses başladı. Məlumat qovluğu: {self.data_dir}")
        
        csv_files = list(self.data_dir.glob('*.csv'))
        
        if not csv_files:
            logger.warning("Göstərilən qovluqda heç bir .csv faylı tapılmadı.")
            return

        for file_path in csv_files:
            table_name = file_path.stem
            logger.info(f"Fayl emal edilir: {file_path.name}")
            
            try:
                df = pd.read_csv(file_path)
            except Exception as e:
                logger.error(f"'{file_path.name}' faylı oxunarkən xəta baş verdi: {e}")
                continue 
            try:
                clean_df = self._clean_dataframe(df)
                self._load_to_db(clean_df, table_name)
            except DataLoadError as e:
                logger.error(f"'{file_path.name}' faylının emalı yarımçıq qaldı: {e}")
                continue


if __name__ == "__main__":
    DB_URI = os.getenv("DATABASE_URI", "mysql+pymysql://root:a25g52a25@localhost/bronze")
    
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR_PATH = Path(os.getenv("DATA_DIR_PATH", BASE_DIR / "data"))

    try:
        pipeline = CsvToSqlPipeline(db_uri=DB_URI, data_dir=DATA_DIR_PATH)
        pipeline.run()
    except Exception as main_error:
        logger.critical(f"Proqramın icrası zamanı kritik xəta baş verdi: {main_error}")