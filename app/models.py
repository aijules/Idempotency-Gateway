"""Request and response schemas for the payment API."""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PaymentRequest(BaseModel):
    """A charge request.

    Validation normalises the amount (``100``, ``100.0`` and ``100.00`` all become
    ``Decimal("100")``) so logically identical payments produce identical fingerprints.
    """

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"examples": [{"amount": 100, "currency": "GHS"}]},
    )

    amount: Annotated[
        Decimal,
        Field(
            gt=0,
            max_digits=12,
            decimal_places=2,
            description="Positive amount in major units, at most 2 decimal places.",
        ),
    ]
    currency: Annotated[
        str,
        Field(pattern=r"^[A-Z]{3}$", description="Three-letter uppercase ISO 4217 code, e.g. GHS."),
    ]

    @field_validator("amount", mode="before")
    @classmethod
    def reject_non_numeric_amount(cls, value: Any) -> Any:
        
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            raise ValueError("amount must be a JSON number")
        return value

    @field_validator("amount")
    @classmethod
    def normalise_amount(cls, value: Decimal) -> Decimal:
        normalised = value.normalize()
       
        if normalised == normalised.to_integral_value():
            return normalised.quantize(Decimal(1))
        return normalised


class PaymentResponse(BaseModel):
    transaction_id: str = Field(examples=["txn_4f1c2a9b8d7e4c3fa1b2c3d4e5f60718"])
    status: str = Field(examples=["Charged 100 GHS"])


class ErrorResponse(BaseModel):
    detail: str
