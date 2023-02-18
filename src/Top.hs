module Top (main) where

import Data.List (intercalate)
import Data.Map (Map)
import Par4 (parse,separated,terminated,alts,many,sat,nl,lit)
import System.Environment (getArgs)
import Text.Printf (printf)
import qualified Data.Map as Map

main :: IO ()
main = do
  args <- getArgs
  let Config {exampleName} = parseArgs args
  let q = getExample exampleName
  let canCompile = exampleName `elem` ["johns","sameSurname"]
  if | canCompile -> do
         let s = schemaOfQuery q
         let a = compile q
         putStrLn (intercalate "," s)
         runActionIO a

     | otherwise -> do
         t <- evalQI q
         putStr (prettyT t)
  where
    getExample :: String -> Query
    getExample x = maybe (error ("no example: "++ x)) id $ Map.lookup x examples

data Config = Config { exampleName :: String }

parseArgs :: [String] -> Config
parseArgs args = do
  case args of
    [] -> Config { exampleName = default_exampleName}
    [exampleName] -> Config { exampleName }
    _ -> error (show ("args",args))
  where
    default_exampleName = "sameSurname"

----------------------------------------------------------------------
-- example queries

examples :: Map String Query
examples = Map.fromList
  [ ("johns",
      project ["Forename","Surname","Party"]
      (Filter (PredEq (RefValue (VString "John")) (RefField "Forename"))
       (ScanFile mps "data/mps.csv")))

  , ("johnsByParty",
      project ["Party","G.Surname"]
      (ExpandAgg "G"
       (GroupBy ["Party"] "G"
        (project ["Forename","Surname","Party"]
         (Filter (PredEq (RefValue (VString "John")) (RefField "Forename"))
          (ScanFile mps "data/mps.csv"))))))

  -- example of slow quadratic join
  , ("sameSurname",
      project ["R.Surname","R.Forename","S.Forename","R.Party","S.Party"]
      (Filter
       (PredAnd
        (PredEq (RefField "R.Surname") (RefField "S.Surname"))
        (PredNe (RefField "R.Forename") (RefField "S.Forename")))
        (Join
          (projectPrefix "R" ["Forename","Surname","Party"] (ScanFile mps "data/mps.csv"))
          (projectPrefix "S" ["Forename","Surname","Party"] (ScanFile mps "data/mps.csv")))))

  -- same example using hash join
  , ("sameSurnameH",
      project ["R.Surname","R.Forename","S.Forename","R.Party","S.Party"]
      (Filter
       (PredNe (RefField "R.Forename") (RefField "S.Forename"))
        (HashJoin ["R.Surname"] ["S.Surname"]
          (projectPrefix "R" ["Forename","Surname","Party"] (ScanFile mps "data/mps.csv"))
          (projectPrefix "S" ["Forename","Surname","Party"] (ScanFile mps "data/mps.csv")))))

  , ("partyMemberCount",
      project ["Party","#members"]
      (CountAgg "G" "#members"
       (GroupBy ["Party"] "G"
         (ScanFile mps "data/mps.csv"))))

  , ("commonName",
      project ["Forename","#Forename"]
      (Filter (PredGt (RefField "#Forename") (RefValue (VInt 6)))
       (CountAgg "G" "#Forename"
        (GroupBy ["Forename"] "G"
          (ScanFile mps "data/mps.csv")))))

  , ("commonNameAcrossParties",
      project ["Forename","F.Party","F.#ForenameByParty"]
      (ExpandAgg "F"
        (Filter (PredGt (RefField "#PartiesWithThanCommonName") (RefValue (VInt 1)))
         (CountAgg "F" "#PartiesWithThanCommonName"
          (GroupBy ["Forename"] "F"
           (Filter (PredGt (RefField "#ForenameByParty") (RefValue (VInt 4)))
            (CountAgg "FP" "#ForenameByParty"
             (GroupBy ["Forename","Party"] "FP"
              (ScanFile mps "data/mps.csv")))))))))
  ]

  where
    project sc = ProjectAs sc sc
    projectPrefix tag sc = ProjectAs sc [ tag++"."++col | col <- sc ]

    mps = ["Forename","Surname","Name (Display as)","Name (List as)","Party","Constituency","Email","Address 1","Address 2","Postcode"]

----------------------------------------------------------------------
-- query language

data Query
  = ScanFile Schema FilePath -- carry schema to avoid file read at compile time
  | ProjectAs Schema Schema Query
  | Filter Pred Query
  | Join Query Query
  | HashJoin Schema Schema Query Query
  | GroupBy Schema ColName Query
  | ExpandAgg ColName Query
  | CountAgg ColName ColName Query
  deriving Show

data Pred
  = PredAnd Pred Pred
  | PredEq Ref Ref
  | PredNe Ref Ref
  | PredGt Ref Ref
  deriving Show

data Ref = RefValue Value | RefField ColName
  deriving Show

----------------------------------------------------------------------
-- compiling queries

schemaOfQuery :: Query -> Schema
schemaOfQuery = sofq
  where
    sofq :: Query -> Schema
    sofq = \case
      ScanFile sc _ -> sc
      ProjectAs _ out _ -> out
      Filter _ sub -> sofq sub
      Join subs1 subs2 -> sofq subs1 ++ sofq subs2
      HashJoin{} -> undefined -- cols1 cols2 sub1 sub2 -> do
      GroupBy{} -> undefined -- cols tag sub -> do
      ExpandAgg{} -> undefined -- aggCol sub -> do
      CountAgg{} -> undefined -- aggCol countCol sub -> do

compile :: Query -> Action
compile q = compile q $ \r -> A_Emit r
  where
    compile :: Query -> (Record -> Action) -> Action
    compile q k = case q of
      ScanFile _ filename -> do
        A_ScanFile filename k

      ProjectAs inp out sub -> do
        compile sub $ \r -> k (renameR inp out r)

      Filter pred sub -> do
        compile sub $ \r -> if evalPred r pred then k r else A_Sequence []

      Join sub1 sub2 -> do
        compile sub1 $ \r1 -> do
          compile sub2 $ \r2 -> do
            k (combineR r1 r2)

      HashJoin{} -> undefined -- cols1 cols2 sub1 sub2 -> do
      GroupBy{} -> undefined -- cols tag sub -> do
      ExpandAgg{} -> undefined -- aggCol sub -> do
      CountAgg{} -> undefined -- aggCol countCol sub -> do

data Action
  = A_Sequence [Action]
  | A_ScanFile FilePath (Record -> Action)
  | A_Emit Record
--  | A_Materialize Action (Table -> Action)
--  | A_ScanTable Table (Record -> Action)

runActionIO :: Action -> IO ()
runActionIO a =
  runI (runActionI a)

runActionI :: Action -> Interaction
runActionI a = loop a I_Done
  where
    loop :: Action -> Interaction -> Interaction
    loop a k = case a of
      A_Sequence [] -> k
      A_Sequence (a:as) -> loop a (loop (A_Sequence as) k)
      A_ScanFile filename f -> do
        I_LoadTable filename $ \(Table rs) ->
          loop (A_Sequence (map f rs)) k
      A_Emit r -> do
        I_Print (prettyR r) k

----------------------------------------------------------------------
-- Interaction (instead of IO)

data Interaction
  = I_Done
  | I_LoadTable FilePath (Table -> Interaction)
  | I_Print String Interaction

runI :: Interaction -> IO ()
runI = loop Map.empty where
  loop :: Map FilePath Table -> Interaction -> IO ()
  loop tMap = \case
    I_Done -> pure ()
    I_LoadTable filename k -> do
      case Map.lookup filename tMap of
        Just tab -> do
          loop tMap (k tab)
        Nothing -> do
          tab <- loadTableFromCSV filename
          loop (Map.insert filename tab tMap) (k tab)
    I_Print s i -> do
      putStrLn s
      loop tMap i

----------------------------------------------------------------------
-- evaluating queries

evalQI :: Query -> IO Table
evalQI = eval
  where
    eval :: Query -> IO Table
    eval = \case
      ScanFile _ filename -> do
        loadTableFromCSV filename
      ProjectAs inp out sub -> do
        t <- eval sub
        pure (renameT inp out t)
      Filter pred sub -> do
        t <- eval sub
        pure (filterT (\r -> evalPred r pred) t)
      Join sub1 sub2 -> do
        t1 <- eval sub1
        t2 <- eval sub2
        pure (crossProductT t1 t2)
      HashJoin cols1 cols2 sub1 sub2 -> do
        t1 <- eval sub1
        t2 <- eval sub2
        pure (hashJoinTables cols1 cols2 t1 t2)
      GroupBy cols tag sub -> do
        t <- eval sub
        pure (groupBy cols tag t)
      ExpandAgg aggCol sub -> do
        t <- eval sub
        pure (expandAgg aggCol t)
      CountAgg aggCol countCol sub -> do
        t <- eval sub
        pure (countAgg aggCol countCol t)

evalPred :: Record -> Pred -> Bool
evalPred r = \case
  PredAnd p1 p2 -> evalPred r p1 && evalPred r p2
  PredEq x1 x2 -> evalRef r x1 == evalRef r x2
  PredNe x1 x2 -> not (evalRef r x1 == evalRef r x2)
  PredGt x1 x2 -> greaterThanV (evalRef r x1) (evalRef r x2)

evalRef :: Record -> Ref -> Value
evalRef r = \case
  RefValue v -> v
  RefField c -> selectR r c

greaterThanV :: Value -> Value -> Bool
greaterThanV v1 v2 = getIntV v1 > getIntV v2

getIntV :: Value -> Int
getIntV = \case
  VInt n -> n
  v -> error ("getIntV, not an int: " ++ show v)

----------------------------------------------------------------------
-- table operations

crossProductT :: Table -> Table -> Table
crossProductT (Table rs1) (Table rs2) =
  Table [ combineR r1 r2 | r1 <- rs1, r2 <- rs2 ]

hashJoinTables :: Schema -> Schema -> Table -> Table -> Table
hashJoinTables cols1 cols2 (Table rs1) (Table rs2) = do
  let m1 :: Map [Value] [Record] =
        Map.fromListWith (++)
        [ (key, [r1])
        | r1 <- rs1
        , let key = map (selectR r1) cols1
        ]
  Table $
    [ combineR r1 r2
    | r2 <- rs2
    , let key = map (selectR r2) cols2
            , r1 <- maybe [] id $ Map.lookup key m1
    ]

filterT :: (Record -> Bool) -> Table -> Table
filterT pred (Table rs) =
  Table [ r | r <- rs, pred r ]

renameT :: Schema -> Schema -> Table -> Table
renameT inp out (Table rs) =
  Table [ renameR inp out r | r <- rs ]

groupBy :: Schema -> ColName -> Table -> Table
groupBy cols tag (Table rs) = do
  let _m1 :: Map [Value] [Record] =
        Map.fromListWith (++)
        [ (key, [r])
        | r <- rs
        , let key = map (selectR r) cols
        ]
  mkTable (cols ++ [tag])
    [ k ++ [VAgg (Table rs)] | (k,rs) <- Map.toList _m1 ]

expandAgg :: ColName -> Table -> Table
expandAgg aggCol (Table rs1) = do
  Table $
    [ combineR r1 (prefixR aggCol r2)
    | r1 <- rs1
    , let VAgg (Table rs2) = selectR r1 aggCol
    , r2 <- rs2
    ]

countAgg :: ColName -> ColName -> Table -> Table
countAgg aggCol countCol (Table rs1) = do
  Table $
    [ combineR r1 rCount
    | r1 <- rs1
    , let VAgg (Table rs2) = selectR r1 aggCol
    , let rCount = Record { schema = [countCol], fields = [ VInt (length rs2) ] }
    ]

----------------------------------------------------------------------
-- record operations

prefixR :: String -> Record -> Record
prefixR tag r@Record{schema} =
  renameR schema (map (\x -> tag++"."++x) schema) r

renameR :: Schema -> Schema -> Record -> Record
renameR inp out r =
  Record { fields = map (selectR r) inp, schema=out }

selectR :: Record -> ColName -> Value
selectR Record{fields,schema} col =
  the who [ v | (c,v) <- zip schema fields, c==col ]
  where who = (show ("select",col,schema))

combineR :: Record -> Record -> Record
combineR Record{fields=f1,schema=s1} Record{fields=f2,schema=s2} =
  Record{fields=f1++f2, schema=s1++s2}

----------------------------------------------------------------------
-- values

data Table = Table [Record]
  deriving (Eq,Ord)

data Record = Record { fields :: [Value], schema :: Schema }
  deriving (Eq,Ord,Show)

type Schema = [ColName]
type ColName = String

data Value = VString String | VInt Int | VAgg Table
  deriving (Eq,Ord)

----------------------------------------------------------------------
-- displaying  values

instance Show Table where
  show = prettyT

instance Show Value where
  show = \case
    VString s -> show s
    VInt n -> show n
    --VAgg t -> show t
    VAgg (Table rs) -> printf "[table:size=%d]" (length rs)

prettyT :: Table -> String
prettyT tab@(Table rs) = case rs of
   [] -> "*empty table*"
   r@Record{schema=sc1}:rs -> do
     if (not (all (== sc1) [ sc2 | Record {schema=sc2} <- rs ]))
       then error (show ( "pretty",tab)) else do
       intercalate "," sc1 ++ "\n"
         ++ unlines [ intercalate "," (map show fields) | Record {fields} <- r:rs ]

prettyR :: Record -> String
prettyR Record{fields} =
  intercalate "," (map show fields)

----------------------------------------------------------------------
-- parsing tables from CVS

loadTableFromCSV :: FilePath -> IO Table
loadTableFromCSV filename = parseCVS <$> readFile filename

parseCVS :: String -> Table
parseCVS = parse table
  where
    table = do
      sc <- separated (lit ',') word; nl
      vss <- terminated nl record
      pure $ mkTable sc vss

    record = map VString <$> separated (lit ',') word

    word = alts [quoted,unquoted]

    quoted = do
      lit '"'
      xs <- many notDoubleQuoteOrNL
      lit '"'
      pure xs

    unquoted = many notCommaOrNL

    notDoubleQuoteOrNL = sat (\case '"' -> False; '\n' -> False; _ -> True)
    notCommaOrNL = sat (\case ',' -> False; '\n' -> False; _ -> True)

----------------------------------------------------------------------
-- misc

mkTable :: Schema -> [[Value]] -> Table
mkTable schema vss = do
  let n = length schema
  Table [ Record { fields = checkLength n vs
                 , schema }
        | vs <- vss ]

checkLength :: Show a => Int -> [a] -> [a]
checkLength n xs =
  if (length xs == n) then xs else
    error (show ("checkLength: expected",n,"got",length xs,xs))

the :: Show a => String -> [a] -> a
the who = \case [a] -> a; as -> error (show ("the",who,as))
