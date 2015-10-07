from databacon import (
  Interface,
  Node,
  Query,
  Schema,
  Alias,
  List,
  String,
  Enum,
  NonNull,
  Self,
  args,
  returns
)

Episode = Enum('Episode', 'NEWHOPE', 'EMPIRE', 'JEDI')


class Character(Interface):
  name = Alias, NonNull, "The character's first and last name"
  friends = [Self]
  appearsIn = [Episode]


class Human(Character):
  homePlanet = String


class Droid(Character):
  primaryFunction = String, 'What it do'


class Root(Query):

  hero = Character
  @hero.args(episode=Episode)
  def _hero(self, episode=None):
    if not episode:
      raise DoTheDefaultThing # fallback to default hydration logic
    return GetHeroForEpisode(episode)

  human = Human
  droid = Droid
  characters = [Character]


