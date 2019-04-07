# qadom

**experimental**

*peer-to-peer social network for question answering*

inspired from Quora and StackOverflow.

## Kesako?

(See below for the english version)

Ce projet vise à créer un réseau social pair-a-pair de
question-réponse. Un système décentralisé sans serveur principal [0].

Contrairement aux services centralises tel que Quora ou StackOverflow,
Qadom n’est pas sous le contrôle d’une (seule) entreprise, ne vous
espionne pas et ne vend pas vos données au plus offrant.

Les outils tel que Mastodon ou Pleroma basés sur les recommandations
du W3C, entre autre la norme ActivityPub, utilisent encore le
paradigme client-serveur pour créer le réseau de fédération, aussi
appelé le Fediverse. Cela veux dire que pour participer à ce(s)
réseau(x) il faut encore qu’une personne bien veillante héberge une
instance d’un de ces logiciels. Un centre vers lequel d’autres
personnes vont se tourner pour héberger leur(s) identité(s) et rendre
possible la conversation.

Qadom est un réseau participatif. Avec Qadom, n’importe qui pourra
héberger sur son ordinateur personnel une partie du réseaux un peu
comme avec le reseau BitTorrent.

Avec Qadom il deviens moins nécessaire l’existence de ces centres [0],
avec l’avantage de toujours garder le contrôle sur son identité.

[0] Il existe (existera) des serveurs “facilitateurs” accessibles
	publiquement sur Internet qui mettent (metteront) en contact les
	différentes pairs. Il sera aussi possible de decouvrir des pairs
	en soundant le reseau local ou directement en entrant les adresses
	de vos amis.

Contrairement aux services basés sur la norme ActivityPub, vous garder
toujours votre identité sur votre ordinateur. Il n’y a pas de notion
d’instance tel que dans le Fediverse, il en suit qu’il n’y pas besoin
d’avoir de compte de secours et qu’il n’est pas nécessaire de migrer
votre profile d’un serveur à un autre car vos données publics sont
partagées par vous et par les autres participants. A moins que le
réseau soit rompu, pour une raison ou une autre, il y a un seul réseau
Qadom.

Qadom un compromis pour avoir de meilleurs performances et
utilisabilité par rapport aux réseaux gnunet, Freenet et Tor… Même si
la résistance aux attaques contre le réseaux pair-a-pair formés par
les participants fait partie des objectifs de Qadom. Qadom privilégie
des interactions riches et proche de l’expérience du Fediverse avec
l’avantage, comme discuté plus haut, d’avoir un plus grand contrôle
sur son identité.

Contrairement à Freenet, gnunet et Tor, pour le moment en tout cas,
Qadom va (uniquement) sécuriser les communications pair-à-pair de
façon à éviter qu’une entité malveillante puisse surveiller le réseau
sans avoir à participer, au moins dans une certaines mesure,
honnêtement au reseau . C’est une mécanique similaire au HTTPS. Une
partie tierce ne peux pas savoir ce que vous dites à votre
interlocuteur, mais votre interlocuteur est au courant de votre
intention. Il existe différente façon de protéger vos intentions à
votre interlocuteur tel faire des demandes fantaisistes (déjà utilisé
dans Qadom pour envoyer les pairs malveillantes au goulag) ou relayer
des demandes d’autres pairs (cela s’appelle “cover traffic” dans
gnunet).

Enfin, il faut remarquer que la sécurité de la vie privée dans Qadom
n’est pas pire que dans le Fediverse (si vous n’utilisez pas Tor (même
si, apparemment, Tor a des failles)).

Je connais peu ou pas IPFS, Secure Scuttlebut (SSB) et DAT. Donc je ne
pourrais pas comparer avec ces réseaux. Une différence avec ses
outils, c’est que Qadom est écrit en Python et que, à priori, c’est
facile à implémenter dans d’autres langages.


## What?

TODO

## How?

To get started, install the dependencies with the following command:

```shell
pip install --user pipenv
git clone https://framagit.org/amz3/qadom.git
cd qadom
pipenv shell
pipenv install --dev
```

Then in a first terminal run:

```shell
pipenv run make devrun
```

In other terminal

```shell
pipenv run make devrun2
```

Then open two browser windows:

```
xdg-open http://localhost:8000
xdg-open http://localhost:8002
```

Don't forget to refresh the page, the application doesn't use
javascript.
