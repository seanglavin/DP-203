import { createRouter, createWebHistory } from 'vue-router'
import Home from '../views/Home.vue'
import PetNameGameView from '../views/games/PetNameGameView.vue'
import MagicCardView from '../views/games/MagicCardView.vue'
import MagicMathGameView from '../views/games/MagicMathGameView.vue'
import MagicPriceGuessView from '../views/games/MagicPriceGuessView.vue'



const routes = [
  {
    path: '/',
    name: 'Home',
    component: Home
  },
  {
    path: '/games/pet-name-game',
    name: 'PetNameGame',
    component: PetNameGameView
  },
  {
    path: '/games/magic-card',
    name: 'MagicCard',
    component: MagicCardView
  },
  {
    path: '/games/magic-math-game',
    name: 'MagicMathGame',
    component: MagicMathGameView
  },
  {
    path: '/games/magic-price-guess',
    name: 'MagicPriceGuess',
    component: MagicPriceGuessView
  },
  // Add more game routes as needed
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router
