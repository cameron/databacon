from databacon import (
  Interface,
  Type,
  Node,
  Query,
  Schema,
  Alias,
  String,
  Enum,
  args,
)

Episode = Enum('Episode', 'NEWHOPE', 'EMPIRE', 'JEDI')


class Character(Interface):
  name = Alias(null=False, desc="The character's first and last name")
  friends = [Type('Character')]
  appearsIn = [Episode()]


class Human(Character):
  homePlanet = String()


class Droid(Character):
  primaryFunction = String('What it do')


class Root(Query):
  hero = Character()

  def _hero(self, episode=Episode):
    return GetHeroForEpisode(episode)

  human = Human()
  droid = Droid()
  characters = [Character()]


