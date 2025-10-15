# booking.py

from feast import (
    Entity,
    FeatureView,
    FileSource,
    Field,
    RequestSource,
    FeatureService,
    PushSource,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float64

import pandas as pd

# =====================================================================================
# == 1. DEFINICIONES DE FUENTES DE DATOS Y ENTIDADES
#    Aquí definimos de dónde vienen nuestros datos y cuál es el sujeto principal.
# =====================================================================================

# El "sujeto" de nuestros datos.
booking = Entity(name="booking", join_keys=["booking_id"])

# La fuente de datos histórica, nuestra "despensa".
booking_source = FileSource(
    name="booking_source",
    path="data/booking_features.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# El mecanismo para "empujar" datos en tiempo real a la fuente anterior.
booking_push_source = PushSource(
    name="booking_push_source",
    batch_source=booking_source,
)

# Define datos que llegarán al momento de la petición, no están pre-calculados.
input_request = RequestSource(
    name="input_request",
    schema=[
        Field(name="kpi1", dtype=Float64),
        Field(name="kpi2", dtype=Float64),
    ],
)


# =====================================================================================
# == 2. DEFINICIONES DE VISTAS DE CARACTERÍSTICAS Y TRANSFORMACIONES
#    Aquí agrupamos las características y definimos la lógica para crear nuevas.
# =====================================================================================

# Grupo lógico de características que vienen directamente de nuestra fuente de datos.
pc_booking_view = FeatureView(
    name="pc_booking_view",
    entities=[booking],
    online=True,
    schema=[
        Field(name="great_feature1", dtype=Float64, description="This is a great feature"),
        Field(name="great_feature2", dtype=Float64, description="This is a great feature"),
    ],
    source=booking_source,
)

# Transformación en tiempo real que combina características existentes con datos de la petición.
@on_demand_feature_view(
    sources=[pc_booking_view, input_request],
    schema=[
        Field(name="great_feature1_kpi1", dtype=Float64),
        Field(name="great_feature2_kpi2", dtype=Float64),
    ],
)
def great_feature_view(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["great_feature1_kpi1"] = inputs["great_feature1"] * inputs["kpi1"]
    df["great_feature2_kpi2"] = inputs["great_feature2"] * inputs["kpi2"]
    return df


# =====================================================================================
# == 3. DEFINICIONES DE SERVICIOS (LA CAPA FINAL)
#    Aquí definimos los "menús" de características que serviremos a los modelos.
# =====================================================================================

# Un "menú" que sirve las características calculadas bajo demanda.
dsrp_feature_service = FeatureService(
    name="dsrp_feature_service",
    features=[great_feature_view],
)

# Un "menú" que sirve solo las características base.
fs_service_pc = FeatureService(
    name="fs_service_pc",
    features=[pc_booking_view],
)