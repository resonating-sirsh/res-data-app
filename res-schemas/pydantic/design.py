from __future__ import annotations
from typing import List, Optional, Union
from enum import Enum
from schemas.pydantic.common import (
    Field,
    # FlowApiModel,
    # CommonConfig,
    # FlowStatus,
    CommonBaseModel,
)

from typing import Optional, Tuple
import uuid
from datetime import datetime
from typing import List, Dict

from pydantic.dataclasses import dataclass


class ColorBase(CommonBaseModel):
    id: uuid.UUID = Field(primary_key=True)
    code: str
    name: str
    brand: str


class DpiUri(CommonBaseModel):
    dpi: int
    uri: str


class Artwork(CommonBaseModel):
    id: uuid.UUID = Field(primary_key=True)
    name: str
    brand: str
    description: Optional[str] = None
    uris: List[DpiUri]
    colors: List[ColorBase] = []
    deleted_at: datetime = Field(alias="deletedAt", default=None)
    # metadata: Optional[Dict[str, Any]] = {}
    # created_at: datetime
    # updated_at: datetime


class CreateArtwork(CommonBaseModel):
    name: str
    brand: str
    description: Optional[str] = None
    s3_uri: Optional[str] = Field(alias="s3Uri")
    tile_preview: Optional[bool] = Field(alias="tilePreview", default=False)
    external_uri: Optional[str] = Field(alias="externalUri")
    # metadata: Dict[str, Any] = {}


class CreateArtworkResponse(CommonBaseModel):
    id: Optional[uuid.UUID]
    dpi: Optional[Tuple[int, int]]
    size: Optional[Tuple[int, int]]
    tile_preview: Optional[str] = Field(alias="tilePreview")
    errors: Optional[List[Dict[str, str]]]


class Body3DFiles(CommonBaseModel):
    id: uuid.UUID = Field(primary_key=True)
    bodyCode: str
    version: int
    model3dUri: Optional[str]
    pointCloudUri: Optional[str]


class Color(CommonBaseModel):
    id: uuid.UUID = Field(primary_key=True)
    code: str
    name: str
    description: str = ""
    brand: str
    # created_at: datetime
    # updated_at: datetime
    artworks: List[Artwork] = []
    deleted_at: datetime = Field(alias="deletedAt", default=None)


class CreateColor(CommonBaseModel):
    code: str
    brand: str
    name: Optional[str]
    description: Optional[str] = ""
    artwork_ids: List[uuid.UUID] = Field(alias="artworkIds", default=[])


class UpdateColor(CreateColor):
    id: Optional[uuid.UUID]


class DesignCatalogItem(CommonBaseModel):
    id: uuid.UUID
    body_code: str = Field(alias="bodyCode")
    body_version: int = Field(alias="bodyVersion")
    color_code: str = Field(alias="colorCode")


class Font(CommonBaseModel):
    id: Optional[uuid.UUID] = Field(primary_key=True)
    name: Optional[str]
    brand: Optional[str]
    uri: Optional[str]  # only because on upload, the uri is not known
    source: Optional[str]  # not needed if updating other stuff
    metadata: Optional[Dict[str, str]]
    created_at: Optional[datetime] = Field(alias="createdAt")
    updated_at: Optional[datetime] = Field(alias="updatedAt")
    deleted_at: Optional[datetime] = Field(alias="deletedAt")


# from style editor
class Geometry(CommonBaseModel):
    type: str
    coordinates: List[List[float]] | List[float]


class GeoJson(CommonBaseModel):
    features: List[GeoJson] | None
    geometry: Optional[Geometry]
    type: Optional[str]


class PieceGeoJsons(CommonBaseModel):
    pieceName: str
    sizeCode: str
    innerGeoJson: GeoJson
    edgesGeoJson: GeoJson
    internalLinesGeoJson: GeoJson
    notchesGeoJson: GeoJson | None
    isBaseSize: bool


# class PieceSizeGeoJsons(CommonBaseModel):
#     pieceSizeGeoJsons: Dict[str, Dict[str, PieceGeoJsons]]


class Pin(CommonBaseModel):
    horizontalFraction: float = 0.5
    verticalFraction: float = 0.5


class TextBox(CommonBaseModel):
    text: str
    fontId: str
    textSizeInches: float
    widthInches: float
    heightInches: float
    rgba: str = "rgba(0, 0, 0, 1)"
    horizontalAlign: Optional[str]
    verticalAlign: Optional[str]


class Placement(CommonBaseModel):
    id: str
    horizontalFraction: float = 0.5
    verticalFraction: float = 0.5
    radians: float = 0
    scale: float = 1
    tiled: bool = False
    zIndex: int = None
    artworkId: str
    artworkInstanceId: int = None
    pieceName: str
    parentId: str = None
    isDefault: bool = False
    sizeScaling: float = None
    pin: Pin = Pin()
    textBox: Optional[TextBox] = None


# class PieceGeometries(CommonBaseModel):
#     pieceName: str
#     sizeCode: str
#     innerPolygon: Dict[str, str]
#     edgeLineStrings: List[Dict[str, str]]
#     isBaseSize: bool


# class PieceSizeGeometries(CommonBaseModel):
#     pieceName: Dict[str, Dict[str, PieceGeometries]]


# class Body(CommonBaseModel):
#     body_code: str
#     version: str
#     id: str


# class Style(CommonBaseModel):
#     id: str
#     body_code: str
#     sku: str
#     body_version: int
#     created_at: str


class PieceDimensions(CommonBaseModel):
    pieceName: str
    width: int
    height: int
    dpi: int
    accurateSource: bool


# class PieceSizeDimensions(CommonBaseModel):
#     pieceSizeDimensions: Dict[str, Dict[str, PieceDimensions]]


# class PieceSizeEdgeGeoJson(CommonBaseModel):
#     pieceSizeEdgeGeoJsons: Dict[str, Dict[str, GeoJson]]


# class PieceSizePlacements(CommonBaseModel):
#     pieceSizePlacements: Dict[str, Dict[str, List[Placement]]]


class GridCoords(CommonBaseModel):
    x: float
    y: float


# class PieceSizeGridCoords(CommonBaseModel):
#     pieceSizeGridCoords: Dict[str, Dict[str, GridCoords]]


class PointAnchorPair(CommonBaseModel):
    horizontalPointType: str
    horizontalPointIndex: int
    horizontalPointSubIndex: int = 0
    verticalPointType: str
    verticalPointIndex: int
    verticalPointSubIndex: int = 0


# class PiecePointAnchorPairs(CommonBaseModel):
#     piecePointAnchorPairs: Dict[str, EdgePointAnchorPair]


class GuideLine(CommonBaseModel):
    type: str
    position: int


class Annotation(CommonBaseModel):
    character: str
    text: str
    pieceName: str
    horizontalFraction: float
    verticalFraction: float
    selected: bool


class PlacementPair(CommonBaseModel):
    id: str
    pieceName: str
    horizontalFraction: float
    verticalFraction: float


class PlacementPairing(CommonBaseModel):
    parent: PlacementPair
    child: PlacementPair


# class PieceSizePlacementPairings(CommonBaseModel):
#     pieceSizePlacementPairings: Dict[str, Dict[str, List[PlacementPairing]]]


# class OffsetPlacement(CommonBaseModel):
#     placement: Placement
#     xOffset: int
#     yOffset: int
#     onPin: bool


class Placeholder(CommonBaseModel):
    name: str = Field(
        description="The human friendly name of the placeholder e.g. Star sign"
    )
    key: Optional[str] = Field(
        description="The key of the placeholder e.g. shoulder_star_sign or logo_star_sign"
    )
    variable: str = Field(
        description="The key of the variable used to get the actual value e.g. horoscope"
    )
    description: Optional[str]
    stage: Optional[str] = Field(
        description="What stage the placeholder is replaced, e.g. DxA/Order"
    )
    type: str = Field(description="An image or text substitution", default="text")
    mapping: Optional[Dict[str, str]] = Field(
        description="map the variable value to a different value, e.g. taurus maps to bull image"
    )
    examples: Optional[List[str]] = Field(
        description="Examples of the placeholder value"
    )


class UserDesign(CommonBaseModel):
    basePlacement: Optional[Placement]
    placements: Dict[str, Dict[str, List[Placement]]]
    placementPairings: Dict[str, Dict[str, List[PlacementPairing]]]
    annotations: Dict[str, Annotation]
    gridCoords: Dict[str, Dict[str, GridCoords]]
    pointAnchorPairs: Dict[str, PointAnchorPair]
    guideLines: Optional[List[GuideLine]]
    placeholders: Optional[Dict[str, Placeholder]]


class DesignData(CommonBaseModel):
    modelUri: str
    pointCloudUri: str
    artworks: Dict[str, Artwork]
    fonts: Dict[str, Font]
    baseSizeCode: str
    basePlacement: Optional[Placement]
    geoJsons: Dict[str, Dict[str, PieceGeoJsons]]
    placements: Dict[str, Dict[str, List[Placement]]]
    placementPairings: Dict[str, Dict[str, List[PlacementPairing]]]
    annotations: Dict[str, Annotation]
    gridCoords: Dict[str, Dict[str, GridCoords]]
    pointAnchorPairs: Dict[str, PointAnchorPair]
    guideLines: Optional[List[GuideLine]]
    sizes: Optional[List[str]]
    bodyCode: str
    bodyVersion: int
    brandCode: str
    bodyId: str
    body3dUris: Optional[Dict[str, Body3DFiles]]
    aliasSizeMapping: Optional[Dict[str, str]]
    placeholders: Optional[Dict[str, Placeholder]]


# class DesignState(CommonBaseModel):
#     artworks: List[Artwork]
#     baseSizeCode: str
#     basePlacement: Placement
#     dimensions: PieceSizeDimensions
#     geometries: PieceSizeGeometries
#     placements: PieceSizePlacements
#     placementPairings: PieceSizePlacementPairings
#     # rulers: Dict[str, Dict[str, bool]]
#     annotations: Dict[str, Annotation]
#     gridCoords: PieceSizeGridCoords
#     pointAnchorPairs: PiecePointAnchorPairs
#     sizes: List[str]


# class Design(CommonBaseModel):
#     baseSizeCode: str
#     artworks: Dict[str, Artwork]
#     dimensions: PieceSizeDimensions
#     geometries: PieceSizeGeometries
#     placements: PieceSizePlacements
#     placementPairings: Dict[str, Dict[int, List[PlacementPairing]]]
#     rulers: Dict[str, Dict[str, bool]]
#     annotations: Dict[str, Annotation]
#     gridCoords: PieceSizeGridCoords
#     pointAnchorPairs: PiecePointAnchorPairs
#     sizes: List[str]
#     clearHistory: callable
#     saveHistory: callable
#     undo: callable
#     redo: callable
#     setBaseSizeCode: callable
#     setBasePlacement: callable
#     addArtwork: callable
#     clearArtworks: callable
#     applyArtworkEverywhere: callable
#     addDimensions: callable
#     clearDimensions: callable
#     clearPlacements: callable
#     addPlacement: callable
#     movePlacement: callable
#     movePin: callable
#     deletePlacement: callable
#     rotatePlacement: callable
#     scalePlacement: callable
#     tilePlacement: callable
#     sizeScalePlacement: callable
#     gradePlacement: callable
#     pairingExists: callable
#     showRulers: callable
#     addRuler: callable
#     clearRulers: callable
#     toggleSelectRuler: callable
#     showAnnotations: callable
#     addAnnotation: callable
#     modifyAnnotation: callable
#     deleteAnnotation: callable
#     clearAnnotations: callable
#     toggleSelectAnnotation: callable
#     setGridCoords: callable
#     setPointAnchorPairs: callable
#     save: callable
#     load: callable
#     shouldShow: Dict[str, bool]
