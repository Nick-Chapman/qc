module Top (main) where

import Data.List (intercalate)

main :: IO ()
main = do
  putStrLn "*qc*"
  let byGod :: Pred = PredEq (RefValue (VString "God")) (RefField "author")
  let q :: Query = Project ["title","pages"] (Filter byGod (Lit books))
  tab <- evalQI q
  putStr (pretty tab)
    where
      books :: Table
      books = Table [r1,r2]
      r1,r2 :: Record
      r1 = Record { schema, fields = [ VString "bible", VString "God", VString "666" ] }
      r2 = Record { schema, fields = [ VString "The God Delusion", VString "Richard Dawkins", VString "420" ] }
      schema = [ "title", "author","pages" ]

data Query
  = Lit Table
  | Load FilePath
  | Project Schema Query
  | ProjectAs Schema Schema Query
  | Filter Pred Query
  deriving Show

data Pred = PredEq Ref Ref | PredNe Ref Ref
  deriving Show

data Ref = RefValue Value | RefField ColName
  deriving Show

evalQI :: Query -> IO Table
evalQI = eval
  where
    eval :: Query -> IO Table
    eval = \case
      Lit tab -> pure tab
      Load{} -> undefined
      Project sc sub -> eval (ProjectAs sc sc sub)
      ProjectAs inp out sub -> do
        Table rs <- eval sub
        pure $ Table [ Record { fields = map (select r) inp, schema=out } | r <- rs ]
      Filter pred sub -> do
        Table rs <- eval sub
        pure $ Table [ r | r <- rs, evalPred r pred ]

evalPred :: Record -> Pred -> Bool
evalPred r = \case
  PredEq x1 x2 -> evalRef r x1 == evalRef r x2
  PredNe x1 x2 -> not (evalRef r x1 == evalRef r x2)

evalRef :: Record -> Ref -> Value
evalRef r = \case
  RefValue v -> v
  RefField c -> select r c

select :: Record -> ColName -> Value
select Record{fields,schema} col = the [ v | (c,v) <- zip schema fields, c==col ]

pretty :: Table -> String
pretty tab@(Table rs) = case rs of
   [] -> "*empty table*"
   r@Record{schema=sc1}:rs -> do
     if (not (all (== sc1) [ sc2 | Record {schema=sc2} <- rs ])) then error (show ( "pretty",tab)) else do
       intercalate "," sc1 ++ "\n"
         ++ unlines [ intercalate "," (map show fields) | Record {fields} <- r:rs ]

data Table = Table [Record]
  deriving Show

data Record = Record { fields :: [Value], schema :: Schema }
  deriving Show

type Schema = [ColName]
type ColName = String

data Value = VString String
  deriving Eq

instance Show Value where
  show = \case
    VString s -> show s

the :: Show a => [a] -> a
the = \case [a] -> a; as -> error (show ("the",as))
