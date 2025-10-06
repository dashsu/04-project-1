from sqlalchemy import MetaData

def get_schema_metadata(engine):
    metadata = MetaData(bind=engine)
    metadata.reflect() 
    return metadata