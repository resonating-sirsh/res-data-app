# PiecePolygon
local square(pieceId=null) = {
  [if std.type(pieceId) != "null" then 'piece']: {id: pieceId},
  points:
    [{ name: 'BL', resIndex: '1',  x: 0, y: 0  },
     { name: 'TL', resIndex: '2',  x: 0, y: 1  },
     { name: 'TR', resIndex: '3',  x: 1, y: 1  },
     { name: 'BR', resIndex: '4',  x: 1, y: 0  },]

};

local pieceEdges = [
  {
    key: '1-2',
    name: 'pe1',
    points: ['1', '2'],
    seamCode: 'FS-0N',
  },
  {
    key: '3-4',
    name: 'pe1',
    points: ['3','4'],
    seamCode: 'CF-1N-0.0625M',
  },
];

local pieceInternalLine = {
  key: 'my connector line',
  startPoint: '1',
  endPoint: '3',
};

local piece(pieceId=null, bodyId=false) = {
  [if std.type(pieceId) != "null" then 'body']: {id: bodyId},
  key: 'skirt',
  name: 'my test square skirt',
  polygon: square(pieceId=pieceId),
  // edges: pieceEdges,
  // internalLines: pieceInternalLine
};

// {"pieces": [piece(pieceId="pieceId123", bodyId="bodyId123")]}
{"pieces": [piece()]}
