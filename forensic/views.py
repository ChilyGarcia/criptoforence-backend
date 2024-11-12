import pandas as pd
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser
from rest_framework import status


class UploadCSVView(APIView):
    parser_classes = [MultiPartParser]

    def post(self, request, *args, **kwargs):
        file = request.FILES.get("file")
        if not file.name.endswith(".csv"):
            return Response(
                {"error": "Invalid file format. Please upload a CSV file."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            # Lee el archivo CSV y convierte la columna 'timestamp' a datetime
            df = pd.read_csv(file)
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df.set_index("timestamp", inplace=True)

            # Genera la tabla de verdad y devuelve la respuesta
            truth_table = self.generate_truth_table(df)
            return Response(truth_table, status=status.HTTP_200_OK)
        except Exception as e:
            return Response(
                {"error": f"Error processing file: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def generate_truth_table(self, df):
        # Métricas de precio
        price_summary = {
            "mean": self.safe_float(df["price"].mean()),
            "max": self.safe_float(df["price"].max()),
            "min": self.safe_float(df["price"].min()),
            "std_dev": self.safe_float(df["price"].std()),
            "median": self.safe_float(df["price"].median()),
            "volatility": self.safe_float(df["price"].pct_change().std() * 100),
        }

        # Métricas de volumen
        volume_summary = {
            "mean": self.safe_float(df["volume"].mean()),
            "max": self.safe_float(df["volume"].max()),
            "min": self.safe_float(df["volume"].min()),
            "std_dev": self.safe_float(df["volume"].std()),
            "total": self.safe_float(df["volume"].sum()),
        }

        # Tendencias temporales (Día/Hora)
        daily_trend = {
            str(date): self.safe_float(price)
            for date, price in df.resample("D")["price"].mean().items()
        }
        hourly_trend = {
            str(date): self.safe_float(price)
            for date, price in df.resample("H")["price"].mean().items()
        }

        # Verifica si existe algún incremento o decremento significativo
        price_trend = [self.safe_float(diff) for diff in df["price"].diff().to_list()]

        # Construye la tabla de verdad con más análisis
        truth_table = {
            "price": price_summary,
            "volume": volume_summary,
            "daily_trend": daily_trend,
            "hourly_trend": hourly_trend,
            "price_trend": price_trend,
        }

        return truth_table

    def safe_float(self, value):
        """Reemplaza valores NaN o infinidades con None para que sean compatibles con JSON."""
        if pd.isna(value) or value == float("inf") or value == float("-inf"):
            return None
        return value
