from schema import Human, Droid, Episode

all_episodes = [Episode.NEWHOPE, Episode.JEDI, Episode.EMPIRE]

luke = Human()
luke.name('Luke Skywalker')
luke.appearsIn.add(all_episodes)
luke.homePlanet('Tatooine')

vader = Human()
vader.name('Darth Vader')
vader.appearsIn.add(all_episodes)
vader.homePlanet('Tatooine')

han = Human()
han.name('Han Solo')
han.appearsIn.add(all_episodes)

leia = Human()
leia.name('Leia Organa')
leia.appearsIn.add(all_episodes)
leia.homePlanet('Alderan')

tarkin = Human()
tarkin.name('Wilhuff Tarkin')
tarkin.appearsIn.add(Episode.NEWHOPE)

threepio = Droid()
threepio.name('C-3P0')
threepio.appearsIn.add(all_episodes)
threepio.primaryFunction('Protocol')

artoo = Droid()
artoo.name('R2-D2')
artoo.appearsIn.add(all_episodes)
artoo.primaryFunction('Astromech')

luke.friends.add([han, leia, artoo, threepio])
vader.friends.add(tarkin)
han.friends.add([luke, leia, artoo, threepio])
leia.friends.add([luke, leia, artoo, threepio])
threepio.friends.add([luke, leia, han, artoo])
artoo.friends.add([luke, leia, han])
