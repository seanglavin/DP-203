<template>
  <div class="home flex flex-col items-center">
    <!-- Games Wrapper -->
    <div class="flex overflow-x-auto space-x-6 py-4 px-4 w-full max-w-full">

      <!-- Game Card for Pet Name Guessing -->
      <div class="game-card bg-[var(--secondary-light)] rounded-xl shadow-md hover:shadow-lg transition w-72 flex-shrink-0 overflow-hidden">
        <h3 class="text-xl font-bold text-center text-[var(--primary-light)] bg-[var(--primary-purple)] py-3 px-4">
          Petfinder Who is that?
        </h3>

        <div class="p-6">
          <!-- <p class="mb-4 text-center">Guess the names of these adoptable pets near you?</p> -->

          <div class="w-full h-48 mb-4 rounded-lg overflow-hidden bg-gray-200 flex items-center justify-center">
            <img
              src="../assets/girl_glare_cropped.png"
              alt="Pet Name Game Preview"
              class="max-w-full max-h-full object-contain"
            />
          </div>

          <router-link
            to="/games/pet-name-game"
            class="block text-center py-2 px-4 bg-[var(--primary-purple)] text-white rounded hover:bg-[var(--secondary-purple)]"
          >
            Play Now
          </router-link>
        </div>
      </div>

      <!-- Game Card for Magic: The Gathering Random Card -->
      <div class="game-card bg-[var(--secondary-light)] rounded-xl shadow-md hover:shadow-lg transition w-72 flex-shrink-0 overflow-hidden">
        <h3 class="text-xl font-bold text-center text-[var(--primary-light)] bg-[var(--primary-purple)] py-3 px-4">
          MTG Random Card
        </h3>
        <div class="p-6">
          <!-- <p class="mb-4 text-center">View a random Magic card from today's selection.</p> -->
          <div class="w-full h-48 mb-4 rounded-lg overflow-hidden bg-gray-200 flex items-center justify-center">
             <img
              src="../assets/Back_of_MTG_card.png"
              alt="Magic Card Display Preview"
              class="max-w-full max-h-full object-contain"
            />
          </div>
          <router-link
            to="/games/magic-card"
            class="block text-center py-2 px-4 bg-[var(--primary-purple)] text-white rounded hover:bg-[var(--secondary-purple)]"
          >
            View Cards
          </router-link>
        </div>
      </div>

      <!-- --- Game Card for Magic Math Game --- -->
      <div class="game-card bg-[var(--secondary-light)] rounded-xl shadow-md hover:shadow-lg transition w-72 flex-shrink-0 overflow-hidden">
        <h3 class="text-xl font-bold text-center text-[var(--primary-light)] bg-[var(--primary-purple)] py-3 px-4">
          MTG Math
        </h3>
        <div class="p-6">
          <!-- <p class="mb-4 text-center">Use card stats to reach the target number!</p> -->
          <div class="w-full h-48 mb-4 rounded-lg overflow-hidden bg-gray-200 flex items-center justify-center">
             <img
              src="../assets/Back_of_MTG_card.png"
              alt="Magic Math Game Preview"
              class="max-w-full max-h-full object-contain"
            />
          </div>
          <router-link
            to="/games/magic-math-game"
            class="block text-center py-2 px-4 bg-[var(--primary-purple)] text-white rounded hover:bg-[var(--secondary-purple)]"
          >
            Play Now
          </router-link>
        </div>
      </div>

      <!-- --- Game Card for Magic Price Guesser --- -->
      <div class="game-card bg-[var(--secondary-light)] rounded-xl shadow-md hover:shadow-lg transition w-72 flex-shrink-0 overflow-hidden">
        <h3 class="text-xl font-bold text-center text-[var(--primary-light)] bg-[var(--primary-purple)] py-3 px-4">
          MTG Price Guesser
        </h3>
        <div class="p-6">
          <!-- <p class="mb-4 text-center">How close can you guess the price of these cards?</p> -->
          <div class="w-full h-48 mb-4 rounded-lg overflow-hidden bg-gray-200 flex items-center justify-center">
             <img
              src="../assets/Back_of_MTG_card.png"
              alt="Magic Price Guesser Preview"
              class="max-w-full max-h-full object-contain"
            />
          </div>
          <router-link
            to="/games/magic-price-guess"
            class="block text-center py-2 px-4 bg-[var(--primary-purple)] text-white rounded hover:bg-[var(--secondary-purple)]"
          >
            Play Now
          </router-link>
        </div>
      </div>

      <!-- Additional Game Cards -->


    </div>
  </div>
</template>

<script>
export default {
  name: 'Home'
}
</script>
